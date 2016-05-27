import asyncio
from typing import Sequence, Callable, Any
from functools import partial
import traceback
import json
import os
import subprocess

from pymongo import MongoClient


class AgentError(Exception):
    pass

class AgentBase:
    # TODO: check mongo return values & exceptions
    def __init__(self, identifier, token, execution_params, mongo: MongoClient):
        self.identifier = identifier
        self.token = token
        self.mongo = mongo

        self.execution_params = execution_params

        # a list of cleanup coroutines for reverting in case of error
        self.stack = []

    def _create_user(self):
        # TODO consider to separate create user and create role
        self.stack.append(self._delete_user)

        db = self.mongo.analysis

        # TODO: do not use token for password, use something urandom

        # create custom role: only readWrite on own collection
        db.command("createRole", self.identifier,
            privileges=[{
               "resource": { "db": "analysis", "collection": self.identifier },
               "actions": ["find", "insert", "remove", "update", "createIndex"]
            }],
            roles=[]
        )

        # create user
        db.add_user(self.identifier, password=self.token,
            roles=[{"role": self.identifier, "db": "analysis" },
                   {"role": "read", "db": "observations"},
                   {"role": "read", "db": "uploads"}])

        print("user created")

    def _delete_user(self):
        db = self.mongo.analysis

        db.remove_user(self.identifier)
        db.command("dropRole", self.identifier)

        self.stack.remove(self._delete_user)
        print("user deleted")

    def _create_collection(self):
        db = self.mongo.analysis

        db.create_collection(self.identifier)

        self.stack.append(self._delete_collection)
        print("collection created")


    def _delete_collection(self):
        db = self.mongo.analysis

        db.drop_collection(self.identifier)

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

    def _handle_request(self, action, payload):
        if action == 'get_info':
            return {
                'url': 'mongodb://{}:{}@localhost/analysis'.format(self.identifier, self.token),
                'output': ('analysis', self.identifier),
                'observations': ('observations', 'observations'),
                'metadata': ('uploads', 'uploads'),
                'execution_params': self.execution_params
            }
        elif action == 'get_spark':
            return {
                'path': '/home/elio/spark-1.6.0-bin-hadoop2.6/',
                'config': {
                    "spark.master": "local[*]",
                    "spark.app.name": "testapp"
                }
            }
        elif action == 'get_distributed':
            return {'address': '127.0.0.1:8706'}
        else:
            return {'error': 'unknown request'}

    def teardown(self):
        raise NotImplementedError()

class OnlineAgent(AgentBase):
    def __init__(self, online_id, token,
                 mongo: MongoClient):

        super().__init__('online_'+str(online_id), token, {}, mongo)

        self.online_id = online_id

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

class ScriptAgent(AgentBase):
    def __init__(self, analyzer_id,
                 action_id, token,
                 execution_params: dict,
                 host, port,
                 cmdline: Sequence[str],
                 cwd: str,
                 mongo: MongoClient):

        super().__init__('script_'+str(action_id), token, execution_params, mongo)

        self.analyzer_id = analyzer_id
        self.action_id = action_id
        self.cmdline = cmdline
        self.cwd = cwd

        # inherit current process environment (default popen behavior) and add credentials
        self.env = dict(os.environ)
        self.env['PTO_CREDENTIALS'] = json.dumps({'identifier': self.identifier, 'token': token, 'host': host, 'port': port})

        self.analyzer_stdout = []
        self.analyzer_stderr = []

        try:
            self._load_analyzer()
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
            self._free_analyzer()
        except:
            self._cleanup()

            # TODO add more info
            raise AgentError()
        else:
            assert(len(self.stack) == 0)


    def _load_analyzer(self):
        self.stack.append(self._free_analyzer)
        print("analyzer loaded")

    def _free_analyzer(self):
        self.stack.remove(self._free_analyzer)
        print("analyzer freed")

    async def execute(self):
        print("executing analyzer...")

        proc = await asyncio.create_subprocess_exec(*self.cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env, cwd=self.cwd)

        stdout, stderr = await proc.communicate()
        self.analyzer_stdout = stdout.decode()
        self.analyzer_stderr = stderr.decode()

        print(self.analyzer_stdout)
        print(self.analyzer_stderr)

        print("retcode", proc.returncode)

        print("analyzer executed")


