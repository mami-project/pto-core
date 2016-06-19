"""
Provides the interface to the supervisor from analyzers written in Python.
"""

import json
import asyncio
import os
from collections import defaultdict
from datetime import datetime
from typing import Sequence, Tuple
from warnings import warn

from pymongo import MongoClient

from .jsonprotocol import JsonProtocol
from . import validator
from .sensitivity import Sensitivity

Interval = Tuple[datetime, datetime]


class ConfigNotFound(Exception):
    pass


class MessageNotUnderstood(Exception):
    pass


class ContextError(Exception):
    pass


class SupervisorClient(JsonProtocol):
    """
    Implements a simple request-response based state-less protocol with token-based
    authentication for passing JSON encoded, line seperated requests to the supervisor.

    Users of this class should only call the method request(), because otherwise the communication
    may break (infinite wait, get answer for different request, etc..).
    """
    def __init__(self, credentials):
        """
        Establish a connection to supervisor but don't exchange any messages yet.
        :param credentials: A dictionary with the keys identifier, token, host, port.
        """
        self.identifier = credentials['identifier']
        self.token = credentials['token']

        self._current = None

        # get fresh and empty event loop
        self.loop = asyncio.new_event_loop()

        # connect to server
        # TODO add tls cert
        coro = self.loop.create_connection(lambda: self, credentials['host'], credentials['port'])
        self.loop.run_until_complete(coro)

    def request(self, action: str, payload: dict = None):
        """
        Send a request (payload optional) to the supervisor.
        :return: The answer dictionary of the supervisor.
        """
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


class AnalyzerContext:
    """
    Almost the first thing an analyzer (both script and online analyzers) wants to do is
    to get access to various data sources and perform activities outside the program.
    The AnalyzerContext is responsible for providing data (MongoDB, HDFS) and
    computing access (Spark, Distributed) to the analyzer.

    Script analyzers need to always call set_result_info() at least once to specify the
    scope of their analysis. Without this information the validator is not able to commit
    to the observations database. For online analyzers this call will yield an
    'unknown request' message from the supervisor.
    """
    def __init__(self, credentials: dict=None, verbose=False):
        """
        Establish a connection to the supervisor given the credentials via constructor parameter
        or environment variables. The existence of an environment variable 'PTO_CREDENTIALS' takes
        precedence over the constructor parameter.
        :param credentials: A dictionary with the keys identifier, token, host, port.
        """
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

        if verbose:
            print("Answer from supervisor:\n"+json.dumps(ans, indent=4))

        self.mongo = MongoClient(ans['mongo_uri'])

        # small helper function for getting the collection
        def get_coll(val):
            return self.mongo[val[0]][val[1]]

        self.temporary_coll = get_coll(ans['temporary_dbcoll'])
        self.temporary_uri = ans['temporary_uri']
        self.observations_coll = get_coll(ans['observations_dbcoll'])
        self.metadata_coll = get_coll(ans['metadata_dbcoll'])
        self.action_log = get_coll(ans['action_log_dbcoll'])

        # get this analyzer's specification
        self.environment = ans['environment']
        self.analyzer_id = ans['analyzer_id']
        self.input_formats = ans['input_formats']
        self.input_types = ans['input_types']
        self.output_types = ans['output_types']
        self.rebuild_all = ans['rebuild_all']

        self.sensitivity = Sensitivity(self.action_log, self.analyzer_id, self.input_formats,
                                       self.input_types, self.rebuild_all)

        # more contexts are loaded on demand
        self._spark_context = None
        self._distributed_executor = None

    def set_result_info(self, max_action_id: int, timespans: Sequence[Interval]):
        timespans_str = [(start_date.isoformat(), end_date.isoformat()) for start_date, end_date in timespans]
        ans = self.supervisor.request('set_result_info', {'max_action_id': max_action_id, 'timespans': timespans_str})
        if 'error' in ans:
            raise ContextError(ans['error'])

    def get_spark(self, verbose=False):
        if self._spark_context is None:
            ans = self.supervisor.request('get_spark')
            if 'error' in ans:
                raise ContextError(ans['error'])

            if verbose:
                print("Answer from supervisor:\n"+json.dumps(ans, indent=4))

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

    def spark_uploads(self, action_id: int, timespans: Sequence[Interval], input_formats: Sequence[str] = None):
        time_subquery = [{'meta.start_time': {'$gte': timespan[0]}, 'meta.stop_time': {'$lte': timespan[1]}}
                         for timespan in timespans]

        action_id_name = 'action_id.'+self.environment

        query = {
            'complete': True,
            action_id_name: {'$lte': action_id},
            'deprecated': False,
            'meta.format': {'$in': input_formats or self.input_formats},
            '$or': time_subquery
        }

        return self.spark_uploads_query(query)

    def spark_uploads_query(self, query):
        sc = self.get_spark()

        if 'complete' not in query or query['complete'] is not True:
            warn("It is strongly advised to include {complete: True} in your query. See manual.")

        action_id_name = 'action_id.'+self.environment
        if action_id_name not in query:
            warn("It is strongly advised to include '"+action_id_name+"' in your query. See manual.")

        uploads = self.metadata_coll.find(query)

        seqfiles = defaultdict(list)
        for upload in uploads:
            seqfiles[upload['path']].append(upload)

        mms = sc.emptyRDD()

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

            mms = mms.union(mm)

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
        return validator.validate(self.analyzer_id, timespans, self.output, output_types, abort_max_errors)
