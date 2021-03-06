"""
Manages the execution of an analyzer (script or online).
"""

import asyncio
from typing import Sequence, Tuple
from datetime import datetime
import json
import os
import subprocess
import logging

import dateutil.parser
from bson.objectid import ObjectId

from .repomanager import clean_repository, get_repository_url_commit
from .coreconfig import CoreConfig

Interval = Tuple[datetime, datetime]


class AgentError(Exception):
    pass


class AnalyzerError(Exception):
    pass


class AgentLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['analyzer_id'], msg), kwargs


class AgentBase:
    """
    Base class provides utility functions for its derived classes such as creating users and temporary
    collections as well as answering analyzer module's requests.
    Do not instantiate directly, use :class:`OnlineAgent` and :class:`ModuleAgent`.

    :param identifier: Username of the module or online analyzer.
    :param token: Authentication token.
    :param core_config: Configuration storage
    :param analyzer_id: Unique identifier of the analyzer module.
    :param input_formats: A list of raw data formats that the analyzer module will read.
    :param input_types: A list of observation types that the analyzer module will read.
    :param output_types: A list of observation types that the analyzer module will write.
    :param git_url: The url of the git repository of the analyzer module.
    :param git_commit: The commit to run the analyzer module with.
    """

    def __init__(self,
                 identifier: str,
                 token: str,
                 core_config: CoreConfig,
                 analyzer_id: str,
                 input_formats: Sequence[str],
                 input_types: Sequence[str],
                 output_types: Sequence[str],
                 git_url: str,
                 git_commit: str):

        self.logger = AgentLoggerAdapter(logging.getLogger("ptocore.supervisor.agent"), {'analyzer_id': analyzer_id})
        self.analyzer_id = analyzer_id
        self.identifier = identifier

        self.token = token
        self.core_config = core_config

        self.input_formats = input_formats
        self.input_types = input_types
        self.output_types = output_types

        # set by set_result_info in analyzercontext. is mandatory for module analyzers, no effect for online analyzers.
        self.result_timespans = None
        self.result_max_action_id = None
        self.result_upload_ids = None

        self.git_url = git_url
        self.git_commit = git_commit

        # a list of cleanup coroutines for reverting in case of error
        self.stack = []

    def _create_user(self):
        """
        Creates the MongoDB user and role to access the observations, core, uploads databases as well as
        the temporary collection where the analyzer module should store its results.

        Put code here if you want to provide more services that require authentication (e.g. spark with auth on).
        """
        self.stack.append(self._delete_user)

        cc = self.core_config

        # create custom role: only readWrite on own collection
        cc.temporary_db.command("createRole", self.identifier,
            privileges=[
                {
                    "resource": {"db": cc.temporary_db.name, "collection": self.identifier},
                    "actions": ["find", "insert", "remove", "update", "createIndex"]
                },
            ],
            roles=[]
        )

        # create user
        cc.temporary_db.add_user(self.identifier, password=self.token,
            roles=[{"role": self.identifier, "db": cc.temporary_db.name},
                   {"role": "read", "db": cc.ptocore_db.name},
                   {"role": "read", "db": cc.observations_db.name},
                   {"role": "read", "db": cc.metadata_db.name}])

        self.logger.info("user created")

    def _delete_user(self):
        """
        Deletes the MongoDB user and role previously created in :func:`_create_user`.
        """
        cc = self.core_config
        cc.temporary_db.remove_user(self.identifier)
        cc.temporary_db.command("dropRole", self.identifier)

        self.stack.remove(self._delete_user)
        self.logger.info("user deleted")

    def _create_collection(self, delete_after=True):
        """
        Creates the temporary collection where the analyzer module should store its results.
        """
        cc = self.core_config
        cc.temporary_db.create_collection(self.identifier)

        if delete_after:
            self.stack.append(self._delete_collection)
        self.logger.info("collection created")


    def _delete_collection(self):
        """
        Delete the collection previously created in :func:`_create_collection`.
        """
        cc = self.core_config
        cc.temporary_db.drop_collection(self.identifier)

        self.stack.remove(self._delete_collection)
        self.logger.info("collection deleted")

    def _cleanup(self):
        """
        Call cleanup functions in the reverse order than they were added.
        """
        for func in reversed(self.stack):
            try:
                func()
            except:
                self.logger.exception("error during cleanup (continuing anyway):", stack_info=True)

        self.logger.info("cleanup done.")

    def _handle_request(self, action: str, payload: dict) -> dict:
        """
        Called by the supervisor from :func:`supervisor.Supervisor._analyzer_request` when a request from the
        analyzer context from the analyzer module was received.

        Provides credentials to services and resources and stores result info.
        :param action: The command of the request.
        :param payload: Additional value depending on action.
        :return: The response message in the form of a dictionary.
        """
        cc = self.core_config
        self.logger.info("requested '{}' with payload '{}'.".format(action, payload))
        if action == 'get_info':
            # get the necessary things to build the mongo URIs
            params = {
                'user': self.identifier,
                'pwd': self.token,
                'host': cc.mongo.address[0],
                'port': cc.mongo.address[1],
                'temp_db': cc.temporary_db.name,
                'temp_coll': self.identifier
            }

            # the URI for simply connecting to the temporary database
            mongo_uri = 'mongodb://{user}:{pwd}@{host}:{port}/{temp_db}'.format(**params)

            # the mongo URI for use with the mongo-hadoop connector
            mongo_temporary_coll_uri = 'mongodb://{user}:{pwd}@{host}:{port}/{temp_db}.{temp_coll}'.format(**params)

            self.logger.debug("returned execution info.")
            return {
                'environment':          cc.environment,
                'mongo_uri':            mongo_uri,
                'temporary_uri':        mongo_temporary_coll_uri,
                'temporary_dbcoll':     (cc.temporary_db.name, self.identifier),
                'observations_dbcoll':  (cc.observations_db.name, cc.observations_coll.name),
                'metadata_dbcoll':      (cc.metadata_db.name, cc.metadata_coll.name),
                'action_log_dbcoll':    (cc.ptocore_db.name, cc.action_log.name),
                'analyzer_id':          self.analyzer_id,
                'input_formats':        self.input_formats,
                'input_types':          self.input_types,
                'output_types':         self.output_types,
                'git_url':              self.git_url,
                'git_commit':           self.git_commit
            }
        elif action == 'get_spark':
            self.logger.debug("returned spark config.")
            return cc.supervisor_spark
        elif action == 'get_distributed':
            self.logger.debug("returned distributed config.")
            return cc.supervisor_distributed
        elif action == 'set_result_info':
            try:
                max_action_id = int(payload['max_action_id'])
                timespans_str = payload['timespans']

                if max_action_id < 0:
                    error = 'max_action_id < 0 not allowed'
                    self.logger.error(error)
                    return {'error': error}

                if len(timespans_str) == 0:
                    error = 'at least one timespan is required'
                    self.logger.error(error)
                    return {'error': error}

                if not all(len(timespan) == 2 and
                           isinstance(timespan[0], str) and
                           isinstance(timespan[1], str) for timespan in timespans_str):
                    error = 'invalid timespans format. expect [("start iso string", "stop iso string"), ...]'
                    self.logger.error(error)
                    return {'error': error}

                timespans = [(dateutil.parser.parse(start_date), dateutil.parser.parse(end_date))
                             for start_date, end_date in timespans_str]

                # TODO compact timespans using timeline

            except (KeyError, ValueError, TypeError) as e:
                error = "one or more fields {'timespans', 'max_action_id'} are invalid or missing:\n"+str(e)
                self.logger.exception(error, stack_info=True)
                return {'error': error}

            self.result_max_action_id = max_action_id
            self.result_upload_ids = None
            self.result_timespans = timespans

            self.logger.debug("got max_action_id: {} and timespans: {}.".format(self.result_max_action_id,
                                                                               self.result_timespans))
            return {'accepted': True}
        elif action == 'set_result_info_direct':
            try:
                max_action_id = int(payload['max_action_id'])
                upload_ids_str = payload['upload_ids']

                if max_action_id < 0:
                    error = 'max_action_id < 0 not allowed'
                    self.logger.error(error)
                    return {'error': error}

                if len(upload_ids_str) == 0:
                    error = 'at least one upload_id is required'
                    self.logger.error(error)
                    return {'error': error}

                upload_ids = [ObjectId(upload_id) for upload_id in upload_ids_str]

            except (KeyError, ValueError, TypeError) as e:
                error = "one or more fields {'timespans', 'max_action_id'} are invalid or missing:\n"+str(e)
                self.logger.exception(error, stack_info=True)
                return {'error': error}

            self.result_max_action_id = max_action_id
            self.result_upload_ids = upload_ids
            self.result_timespans = None

            self.logger.debug("got max_action_id: {} and timespans: {}.".format(self.result_max_action_id,
                                                                                self.result_upload_ids))
            return {'accepted': True}
        else:
            self.logger.error("don't know how to handle the request.")
            return {'error': 'unknown request'}


    def teardown(self):
        raise NotImplementedError()

class OnlineAgent(AgentBase):
    """
    Agent for explorative (i.e. REPL-style) analysis.
    Do not create directly, use `create_online_agent()` in :class:`.supervisor.Supervisor`

    :param identifier: Username of the module or online analyzer.
    :param token: Authentication token.
    :param core_config: Configuration storage
    """
    def __init__(self, identifier, token, core_config: CoreConfig):

        super().__init__(identifier, token, core_config, identifier, [], [], [], '', '')

        self.logger.info("online agent created")

        try:
            self._create_collection()
            self._create_user()
        except:
            self._cleanup()

            # TODO add more info
            raise AgentError()

    def teardown(self):
        try:
            self._delete_user()
            self._delete_collection()
        except:
            self._cleanup()

            # TODO add more info
            raise AgentError()
        else:
            assert(len(self.stack) == 0)


class ModuleAgent(AgentBase):
    """
    Agent for analyzer module execution.
    Executes ``cmdline`` in ``working_dir`` using via asyncio subprocess.

    :param analyzer_id: Unique identifier of the analyzer module.
    :param identifier: Username of the module or online analyzer.
    :param token: Authentication token.
    :param core_config: Configuration storage
    :param input_formats: A list of raw data formats that the analyzer module will read.
    :param input_types: A list of observation types that the analyzer module will read.
    :param output_types: A list of observation types that the analyzer module will write.
    :param working_dir: Directory of the analyzer module repository.
    :param ensure_clean_repo: Whether repository should be cleaned before execution.
                              Should be true for production environments.
    """
    def __init__(self,
                 analyzer_id: str,
                 identifier: str,
                 token: str,
                 core_config: CoreConfig,
                 input_formats: Sequence[str],
                 input_types: Sequence[str],
                 output_types: Sequence[str],
                 cmdline: Sequence[str],
                 working_dir: str,
                 ensure_clean_repo: bool):

        git_url, git_commit = get_repository_url_commit(working_dir)

        super().__init__(identifier, token, core_config, analyzer_id,
                         input_formats, input_types, output_types, git_url, git_commit)

        self.cmdline = cmdline
        self.working_dir = working_dir

        # inherit current process environment (this is the default popen behavior) and add credentials
        self.env = dict(os.environ)
        creds = {
            'identifier': self.identifier,
            'token': token,
            'host': 'localhost',
            'port': core_config.supervisor_port
        }
        self.env['PTO_CREDENTIALS'] = json.dumps(creds)

        self.analyzer_stdout = []
        self.analyzer_stderr = []

        self.logger.info("module agent created with identifier {}.".format(identifier))

        try:
            if ensure_clean_repo:
                self.logger.info("clean repository")
                clean_repository(working_dir)
            self._create_collection(delete_after=False)
            self._create_user()
        except:
            self._cleanup()

            # TODO add more info
            raise AgentError()

    def teardown(self):
        try:
            self._delete_user()
            # deleting the collection is done in the validator
        except:
            self._cleanup()

            # TODO add more info
            raise AgentError()
        else:
            assert(len(self.stack) == 0)

    async def execute(self):
        """
        Coroutine which executes the analyzer module and prints stdio and stderr to log.
        """
        self.logger.info(
            "executing analyzer with command line '{}' in working dir '{}'"
                .format(self.cmdline, self.working_dir)
        )

        proc = await asyncio.create_subprocess_exec(*self.cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                                    env=self.env, cwd=self.working_dir)

        stdout, stderr = await proc.communicate()
        self.analyzer_stdout = stdout.decode()
        self.analyzer_stderr = stderr.decode()

        self.logger.info(self.analyzer_stdout)
        self.logger.info(self.analyzer_stderr)

        if proc.returncode != 0:
            raise AnalyzerError("The analyzer return value was not zero.")

        self.logger.info("analyzer executed")


