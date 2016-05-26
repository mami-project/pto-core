import asyncio
from typing import Sequence, Callable, Any
from pymongo import MongoClient
from functools import partial
import traceback
import json
import os
import subprocess

from supervisor import Credentials

class AgentError(Exception):
    pass

class AgentBase:
    # TODO: check mongo return values & exceptions
    def __init__(self, identifier, token, mongo: MongoClient):
        self.identifier = identifier
        self.token = token
        self.mongo = mongo

        # a list of cleanup coroutines for reverting in case of error
        self.stack = []

    def _create_user(self):
        # TODO consider to separate create user and create role
        self.stack.append(self._delete_user)

        db = self.mongo.analysis

        # TODO: do not use token for password, use something urandom

        # create custom role: only readWrite on own collection
        db.command("createRole", 'analyzer_'+self.identifier,
            privileges=[{
               "resource": { "db": "analysis", "collection": self.identifier },
               "actions": ["find", "insert", "remove", "update", "createIndex"]
            }],
            roles=[]
        )

        # create user
        db.add_user("analyzer_"+self.identifier, password=self.token,
            roles=[{"role": "analyzer_"+self.identifier, "db": "analysis" },
                   {"role": "read", "db": "observations"},
                   {"role": "read", "db": "uploads"}])

        print("user created")

    def _delete_user(self):
        db = self.mongo.analysis

        db.remove_user("analyzer_"+self.identifier)
        db.command("dropRole", "analyzer_"+self.identifier)

        self.stack.remove(self._delete_user)
        print("user deleted")

    def _create_collection(self):
        db = self.mongo.analysis

        db._create_collection(self.identifier)

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

    def _handle_request(self, req):
        if req['req'] == 'get_mongo':
            return {
                'url': 'mongodb://{}:{}@localhost/analysis'.format('analyzer_'+self.identifier, self.token),
                'output': ('analysis', self.identifier),
                'observations': ('observations', 'observations'),
                'metadata': ('uploads', 'uploads')
            }
        elif req['req'] == 'get_spark':
            return {
                'path': '../spark-1.6.0-bin-hadoop2.6/',
                'config': {
                    "spark.master": "local[*]",
                    "spark.app.name": "testapp"
                }
            }
        elif req['req'] == 'get_distributed':
            return {'address': '127.0.0.1:8706'}
        else:
            return {'error': 'unknown request'}

    def teardown(self):
        raise NotImplementedError()

class OnlineAgent(AgentBase):
    def __init__(self, identifier, token,
                 mongo: MongoClient):

        super().__init__(identifier, token, mongo)

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
                 credentials: Credentials,
                 cmdline: Sequence[str],
                 mongo: MongoClient):
        super().__init__(credentials.identifier, credentials.token, mongo)

        self.analyzer_id = analyzer_id
        self.cmdline = cmdline

        self.credentials = credentials

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
        # assuming AnalyzerServer is run by supervisor

        # inherit current process environment (default behavior) and add credentials
        env = dict(os.environ)

        env['PTO_CREDENTIALS'] = json.dumps(self.credentials._asdict())

        proc = await asyncio.create_subprocess_exec(*self.cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

        stdout, stderr = await proc.communicate()
        self.analyzer_stdout = stdout.decode()
        self.analyzer_stderr = stderr.decode()

        print("retcode", proc.returncode)

        print("analyzer executed")


