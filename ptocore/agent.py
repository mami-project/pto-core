"""
Manages the execution of an analyzer (script or online).
"""

import asyncio
from typing import Sequence, Tuple
from datetime import datetime
import traceback
import json
import os
import subprocess

import dateutil.parser

from .repomanager import clean_repository
from .coreconfig import CoreConfig

Interval = Tuple[datetime, datetime]


class AgentError(Exception):
    pass


class AnalyzerError(Exception):
    pass


class AgentBase:
    # TODO: check mongo return values & exceptions
    def __init__(self, identifier, token, core_config: CoreConfig, analyzer_id: str,
                 input_formats: Sequence[str], input_types: Sequence[str], output_types: Sequence[str],
                 rebuild_all: bool=False):
        self.analyzer_id = analyzer_id
        self.identifier = identifier

        self.token = token
        self.core_config = core_config

        self.input_formats = input_formats
        self.input_types = input_types
        self.output_types = output_types

        self.rebuild_all = rebuild_all

        # set by set_result_info in analyzercontext. is mandatory for module analyzers, no effect for online analyzers.
        self.result_timespans = None
        self.result_max_action_id = None

        # a list of cleanup coroutines for reverting in case of error
        self.stack = []

    def _create_user(self):
        # TODO consider to separate create user and create role
        self.stack.append(self._delete_user)

        # TODO: do not use token for password, use something urandom
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

        print("user created")

    def _delete_user(self):
        cc = self.core_config
        cc.temporary_db.remove_user(self.identifier)
        cc.temporary_db.command("dropRole", self.identifier)

        self.stack.remove(self._delete_user)
        print("user deleted")

    def _create_collection(self, delete_after=True):
        cc = self.core_config
        cc.temporary_db.create_collection(self.identifier)

        if delete_after:
            self.stack.append(self._delete_collection)
        print("collection created")


    def _delete_collection(self):
        cc = self.core_config
        cc.temporary_db.drop_collection(self.identifier)

        self.stack.remove(self._delete_collection)
        print("collection deleted")

    def _cleanup(self):
        for func in reversed(self.stack):
            try:
                func()
            except:
                # TODO: log problem and continue cleanup
                print("Error during cleanup:")
                traceback.print_exc()
                print("Continuing cleanup..")

        print("Cleanup done.")

    def _handle_request(self, action: str, payload: dict):
        cc = self.core_config
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

            return {
                'environment':          cc.environment,
                'mongo_uri':            mongo_uri,
                'temporary_uri':        mongo_temporary_coll_uri,
                'temporary_dbcoll':     (cc.temporary_db.name, self.identifier),
                'observations_dbcoll':  (cc.observations_db.name, cc.observations_db.name),
                'metadata_dbcoll':      (cc.metadata_db.name, cc.metadata_coll.name),
                'action_log_dbcoll':    (cc.ptocore_db.name, cc.action_log.name),
                'analyzer_id':          self.analyzer_id,
                'input_formats':        self.input_formats,
                'input_types':          self.input_types,
                'output_types':         self.output_types,
                'rebuild_all':          self.rebuild_all
            }
        elif action == 'get_spark':
            return cc.supervisor_spark
        elif action == 'get_distributed':
            return cc.supervisor_distributed
        elif action == 'set_result_info':
            try:
                max_action_id = int(payload['max_action_id'])
                timespans_str = payload['timespans']

                if max_action_id < 0:
                    return {'error': 'max_action_id < 0 not allowed'}

                if len(timespans_str) == 0:
                    return {'error': 'at least one timespan is required'}

                if not all(len(timespan) == 2 and
                           isinstance(timespan[0], str) and
                           isinstance(timespan[1], str) for timespan in timespans_str):
                    return {'error': 'invalid payload format'}

                timespans = [(dateutil.parser.parse(start_date), dateutil.parser.parse(end_date))
                             for start_date, end_date in timespans_str]

            except (KeyError, ValueError, TypeError) as e:
                traceback.print_exc()
                return {'error': "one or more fields {'timespans', 'max_action_id'} are invalid or missing:\n"+str(e)}


            self.result_max_action_id = max_action_id
            self.result_timespans = timespans

            return {'accepted': True}
        else:
            return {'error': 'unknown request'}


    def teardown(self):
        raise NotImplementedError()

class OnlineAgent(AgentBase):
    def __init__(self, identifier, token, core_config: CoreConfig):

        super().__init__(identifier, token, core_config, identifier, [], [], [])

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
    def __init__(self, analyzer_id,
                 identifier, token,
                 core_config: CoreConfig,
                 input_formats: Sequence[str],
                 input_types: Sequence[str],
                 output_types: Sequence[str],
                 cmdline: Sequence[str],
                 working_dir: str,
                 rebuild_all: bool,
                 ensure_clean_repo: bool):

        super().__init__(identifier, token, core_config, analyzer_id,
                         input_formats, input_types, output_types, rebuild_all)

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

        try:
            if ensure_clean_repo:
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
        print("executing analyzer...")

        proc = await asyncio.create_subprocess_exec(*self.cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env, cwd=self.working_dir)

        stdout, stderr = await proc.communicate()
        self.analyzer_stdout = stdout.decode()
        self.analyzer_stderr = stderr.decode()

        print(self.analyzer_stdout)
        print(self.analyzer_stderr)

        if proc.returncode != 0:
            raise AnalyzerError("The analyzer return value was not zero.")

        print("analyzer executed")


