import json
import asyncio

import os
from pymongo import MongoClient
from .jsonprotocol import JsonProtocol

class ConfigNotFound(Exception):
    pass


class MessageNotUnderstood(Exception):
    pass


class ContextError(Exception):
    pass


class SupervisorClient(JsonProtocol):
    """
    Simple request-answer based client.
    """
    def __init__(self, credentials):
        self.identifier = credentials['identifier']
        self.token = credentials['token']

        # get fresh and empty event loop
        self.loop = asyncio.new_event_loop()

        # connect to server
        coro = self.loop.create_connection(lambda: self, credentials['host'], credentials['port'])
        self.loop.run_until_complete(coro)

    def request(self, action: str, payload: dict = None):
        msg = {
            'identifier': self.identifier,
            'token': self.token,
            'action': action,
            'payload': payload
        }

        # send request
        self.send(msg)

        # wait until self.received() is called and _current contains the response
        # NOTE: obviously this pattern will fail if the server doesn't send exactly the same number of
        # answers than the number of received requests.
        self.loop.run_forever()
        return self._current

    def received(self, obj):
        self._current = obj
        self.loop.stop()


class AnalyzerContext():
    def __init__(self, credentials: dict=None):
        # environment variable overrides parameter
        if 'PTO_CREDENTIALS' in os.environ:
            credentials = json.loads(os.environ['PTO_CREDENTIALS'])

        if credentials is None:
            raise ConfigNotFound()

        self.supervisor = SupervisorClient(credentials)

        # authenticate and get mongodb login data
        ans = self.supervisor.request('get_info')
        if 'error' in ans:
            raise ContextError(ans['error'])

        self.mongo = MongoClient(ans['url'])

        self.output_db = self.mongo[ans['output'][0]]
        self.output = self.output_db[ans['output'][1]]

        self.observations_db = self.mongo[ans['observations'][0]]
        self.observations = self.observations_db[ans['observations'][1]]

        self.metadata_db = self.mongo[ans['metadata'][0]]
        self.metadata = self.metadata_db[ans['metadata'][1]]

        self.parameters = ans['execution_params']

        self._spark_context = None
        self._distributed_executor = None


    def get_spark(self):
        if self._spark_context is None:
            ans = self.supervisor.request('get_spark')
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
            ans = self.supervisor.request('get_distributed')
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
