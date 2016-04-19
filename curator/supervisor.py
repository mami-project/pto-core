import asyncio
from pymongo import MongoClient
import traceback
import json
import os
import subprocess

class SupervisorError(Exception):
    pass

class SupervisorBase:
    # TODO: check mongo return values & exceptions
    def __init__(self, identifier, token, mongo: MongoClient):
        self.identifier = identifier
        self.token = token
        self.mongo = mongo

        # a list of cleanup coroutines for reverting in case of error
        self.stack = []

    async def create_user(self):
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

    async def delete_user(self):
        db = self.mongo.analysis

        db.remove_user("analyzer_"+self.identifier)
        db.command("dropRole", "analyzer_"+self.identifier)

        self.stack.remove(self.delete_user)
        print("user deleted")

    async def create_collection(self):
        db = self.mongo.analysis

        db.create_collection(self.identifier)

        self.stack.append(self.delete_collection)
        print("collection created")


    async def delete_collection(self):
        db = self.mongo.analysis

        db.drop_collection(self.identifier)

        self.stack.remove(self.delete_collection)
        print("collection deleted")


    async def load_rawdata(self):
        self.stack.append(self.free_rawdata)
        print("rawdata loaded")

    async def free_rawdata(self):
        self.stack.remove(self.free_rawdata)
        print("rawdata freed")

    async def load_validator(self):
        self.stack.append(self.free_validator)
        print("validator loaded")

    async def exec_validator(self):
        print("validator executed")

    async def free_validator(self):
        self.stack.remove(self.free_validator)
        print("validator freed")

    async def lock_types(self):
        self.stack.append(self.free_types)
        print("types locked")

    async def free_types(self):
        self.stack.remove(self.free_types)
        print("types freed")

    async def cleanup(self):
        for corofunc in reversed(self.stack):
            try:
                await corofunc()
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
            return {'address': 'localhost:8706'}
        else:
            return {'error': 'unknown request'}

class SupervisorOnline(SupervisorBase):
    def __init__(self, identifier, token, mongo):
        super().__init__(identifier, token, mongo)

    async def startup(self):
        try:
            await self.lock_types()
            await self.load_rawdata()
            await self.load_validator()
            await self.create_collection()
            await self.create_user()
        except:
            await self.cleanup()

            # TODO add more info
            raise SupervisorError()

    async def teardown(self):
        try:
            await self.delete_user()
            await self.free_rawdata()
            await self.free_validator()
            await self.delete_collection()
            await self.free_types()
        except:
            await self.cleanup()

            # TODO add more info
            raise SupervisorError()
        else:
            assert(len(self.stack) == 0)

class SupervisorScript(SupervisorBase):
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
        # assuming AnalyzerServer is run by curator

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
            await self.lock_types()
            await self.load_rawdata()
            await self.load_analyzer()
            await self.load_validator()
            await self.create_collection()
            await self.create_user()

            # stage 2: execute analyzer
            await self.exec_analyzer()

            # stage 3: prepare for validation
            await self.delete_user()
            await self.free_rawdata()
            await self.free_analyzer()

            # stage 4: execute validator
            await self.exec_validator()

            # stage 5: finalizing
            await self.free_validator()

            await self.commit()
            await self.delete_collection()
            await self.free_types()
        except:
            await self.cleanup()
            return False
        else:
            assert(len(self.stack) == 0)
            return True