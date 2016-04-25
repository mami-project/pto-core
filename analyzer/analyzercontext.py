import os
import json
import asyncio
from jsonprotocol import JsonProtocol
from pymongo import MongoClient

class ConfigNotFound(Exception):
    pass

class MessageNotUnderstood(Exception):
    pass

class ContextError(Exception):
    pass

class AnalyzerClient(JsonProtocol):
    def __init__(self, host, port, identifier, token):
        self.identifier = identifier
        self.token = token
        self.loop = asyncio.get_event_loop()
        coro = self.loop.create_connection(lambda: self, host, port)
        self.loop.run_until_complete(coro)

    def recv(self):
        self.loop.run_forever()
        return self._current

    def request(self, action: str, obj: dict = None):
        payload = obj.copy() if obj is not None else {}

        payload['req'] = action
        payload['identifier'] = self.identifier
        payload['token'] = self.token

        self.send(payload)
        return self.recv()

    def received(self, obj):
        self._current = obj
        self.loop.stop()

class AnalyzerContext():
    def __init__(self, bootstrap_config_fn=None):
        if 'PTO_BOOTSTRAP' in os.environ:
            bootstrap = json.loads(os.environ['PTO_BOOTSTRAP'])
        elif bootstrap_config_fn is not None:
            bootstrap = json.loads(open(bootstrap_config_fn).read())
        else:
            raise ConfigNotFound()

        self.curator = AnalyzerClient(bootstrap['host'], bootstrap['port'], bootstrap['identifier'], bootstrap['token'])

        # authenticate and get mongo credentials
        ans = self.curator.request('get_mongo')
        if 'error' in ans:
            raise ContextError(ans['error'])

        self.mongo = MongoClient(ans['url'])

        self.output_db = self.mongo[ans['output'][0]]
        self.output = self.output_db[ans['output'][1]]

        self.observations_db = self.mongo[ans['observations'][0]]
        self.observations = self.observations_db[ans['observations'][1]]

        self.metadata_db = self.mongo[ans['metadata'][0]]
        self.metadata = self.metadata_db[ans['metadata'][1]]

        self._spark_context = None
        self._distributed_executor = None

    def get_spark(self):
        if self._spark_context is None:
            ans = self.curator.request('get_spark')
            if 'error' in ans:
                raise ContextError(ans['error'])

            # path to spark files
            spark_path = ans['path']

            # the dictionary stored in ans['spark']['config'] will be given to SparkConf directly
            # e.g. { "spark.master": "local[5]", "spark.app.name": "testapp" }
            spark_config = ans['config']

            import findspark
            findspark.init(spark_path)

            import pyspark
            import pymongo_spark

            pymongo_spark.activate()

            conf = pyspark.SparkConf()
            conf.setAll(spark_config.items())

            self._spark_context = pyspark.SparkContext()

        return self._spark_context

    def get_distributed(self):
        if self._distributed_executor is None:
            ans = self.curator.request('get_distributed')
            if 'error' in ans:
                raise ContextError(ans['error'])

            from distributed import Executor
            self._distributed_executor = Executor(ans['address'])

        return self._distributed_executor

    def validate(self):
        # TODO: implement
        # production: supervisor will revoke user rights and check if disconnected
        # ans = self.supervisor.request('validate')

        # close if in production mode
        # self.mongo.close()

        raise NotImplementedError()
