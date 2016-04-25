import asyncio
from pymongo import MongoClient
import traceback
import json
import os
import subprocess

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

    def create_user(self):
        # TODO consider to separate create user and create role
        self.stack.append(self.delete_user)

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

    def delete_user(self):
        db = self.mongo.analysis

        db.remove_user("analyzer_"+self.identifier)
        db.command("dropRole", "analyzer_"+self.identifier)

        self.stack.remove(self.delete_user)
        print("user deleted")

    def create_collection(self):
        db = self.mongo.analysis

        db.create_collection(self.identifier)

        self.stack.append(self.delete_collection)
        print("collection created")


    def delete_collection(self):
        db = self.mongo.analysis

        db.drop_collection(self.identifier)

        self.stack.remove(self.delete_collection)
        print("collection deleted")


    async def load_rawdata(self):
        self.stack.append(self.free_rawdata)
        print("rawdata loaded")

    def free_rawdata(self):
        self.stack.remove(self.free_rawdata)
        print("rawdata freed")

    def lock_types(self):
        self.stack.append(self.free_types)
        print("types locked")

    def free_types(self):
        self.stack.remove(self.free_types)
        print("types freed")

    async def cleanup(self):
        for func in reversed(self.stack):
            try:
                if asyncio.iscoroutinefunction(func):
                    await func()
                else:
                    func()
            except:
                # TODO: log problem and continue cleanup
                print("Error during cleanup:")
                traceback.print_exc()
                print("Continuing cleanup..")

        print("Cleanup done.")

    def handle_request(self, req):
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

class OnlineAgent(AgentBase):
    def __init__(self, identifier, token, mongo):
        super().__init__(identifier, token, mongo)

    async def startup(self):
        try:
            await self.load_rawdata()
            self.create_collection()
            self.create_user()
        except:
            await self.cleanup()

            # TODO add more info
            raise AgentError()

    async def teardown(self):
        try:
            self.delete_user()
            self.delete_collection()
            self.free_rawdata()
        except:
            await self.cleanup()

            # TODO add more info
            raise AgentError()
        else:
            assert(len(self.stack) == 0)

class ScriptAgent(AgentBase):
    def __init__(self, bootstrap, cmdline, mongo):
        super().__init__(bootstrap['identifier'], bootstrap['token'], mongo)

        self.cmdline = cmdline
        self.bootstrap = bootstrap

        self.analyzer_stdout = []
        self.analyzer_stderr = []

    async def load_analyzer(self):
        self.stack.append(self.free_analyzer)
        print("analyzer loaded")

    async def exec_analyzer(self):
        # assuming AnalyzerServer is run by supervisor

        # inherit current process environment (default behavior) and add bootstrap information
        env = dict(os.environ)
        env['PTO_BOOTSTRAP'] = json.dumps(self.bootstrap)

        proc = await asyncio.create_subprocess_exec(*self.cmdline, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

        stdout, stderr = await proc.communicate()
        self.analyzer_stdout = stdout.decode()
        self.analyzer_stderr = stderr.decode()

        print("retcode", proc.returncode)

        print("analyzer executed")

    async def free_analyzer(self):
        self.stack.remove(self.free_analyzer)
        print("analyzer freed")

    async def commit(self):
        print("committed")

    async def run(self):
        try:
            # stage 1: prepare
            self.lock_types()
            await self.load_rawdata()
            await self.load_analyzer()
            self.create_collection()
            self.create_user()

            # stage 2: execute analyzer
            await self.exec_analyzer()

            # stage 3: prepare for validation
            self.delete_user()
            self.free_rawdata()
            self.free_analyzer()

            # TODO: move to validator
            #await self.commit()
            #await self.delete_collection()
            #await self.free_types()
        except:
            await self.cleanup()
            return False
        else:
            assert(len(self.stack) == 0)
            return True