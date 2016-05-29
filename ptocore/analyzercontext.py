import json
import asyncio
import os
from collections import defaultdict
from datetime import datetime
from typing import Sequence, Tuple

from pymongo import MongoClient

from .jsonprotocol import JsonProtocol
from . import validator

Interval = Tuple[datetime, datetime]

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

        # connection to supervisor
        self.supervisor = SupervisorClient(credentials)

        # authenticate and get mongodb details
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

        # get this analyzer's specification
        self.analyzer_id = ans['analyzer_id']
        self.action_id = ans['action_id']
        self.input_formats = ans['input_formats']
        self.input_types = ans['input_types']
        self.output_types = ans['output_types']

        # other contexts loaded on demand
        self._spark_context = None
        self._distributed_executor = None

    def set_result_info(self, max_action_id: int, timespans: Sequence[Interval]):
        ans = self.supervisor.request('set_result_info', {'max_action_id': max_action_id,
                                                          'timespans': timespans})
        if 'error' in ans:
            raise ContextError(ans['error'])

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

    def spark_get_uploads(self, query):
        sc = self.get_spark()

        assert('complete' in query and query['complete'] is True)

        uploads = self.metadata.find(query)

        seqfiles = defaultdict(list)
        for upload in uploads:
            seqfiles[upload['path']].append(upload)

        mms = None

        # build up chain of operations
        for seqfile, uploads in seqfiles.items():
            # create rdd of the metadata with seqKey: metadata
            metadata = sc.parallelize(uploads).map(lambda upload: (upload['seqKey'], upload))

            # sequence file is a binary key-value store. by definition the measurements are stored with seqKey as key.
            textfiles = sc.sequenceFile(seqfile).map(lambda kv: (kv[0].decode('utf-8'), kv[1].decode('utf-8')))

            # this inner join does two things:
            # 1. keep only measurements we need
            # 2. put metadata alongside each measurement
            mm = metadata.join(textfiles)

            if mms is None:
                mms = mm
            else:
                mms.union(mm)

        return mms


    def get_distributed(self):
        if self._distributed_executor is None:
            ans = self.supervisor.request('get_distributed')
            if 'error' in ans:
                raise ContextError(ans['error'])

            from distributed import Executor
            self._distributed_executor = Executor(ans['address'])

        return self._distributed_executor

    def validate(self, timespans: Sequence[Interval], output_types: Sequence[str], abort_max_errors=100):
        return validator.validate(self.analyzer_id, self.action_id, timespans,
                                  self.output, output_types, abort_max_errors)
