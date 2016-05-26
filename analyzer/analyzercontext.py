import os
import json
from jsonprotocol import JsonProtocol
from pymongo import MongoClient
from supervisor import SupervisorClient


class ConfigNotFound(Exception):
    pass


class MessageNotUnderstood(Exception):
    pass


class ContextError(Exception):
    pass

class AnalyzerContext():
    def __init__(self, bootstrap: Bootstrap=None):
        # environment variable overrides parameter
        if 'PTO_CREDENTIALS' in os.environ:
            bootstrap = json.loads(os.environ['PTO_CREDENTIALS'])

        if bootstrap is None:
            raise ConfigNotFound()

        self.supervisor = SupervisorClient(bootstrap.host, bootstrap.port, bootstrap.identifier, bootstrap.token)

        # authenticate and get mongo credentials
        ans = self.supervisor.request('get_mongo')
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
