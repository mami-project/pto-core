from datetime import datetime
from typing import Sequence, Tuple
from time import sleep

import re
from bson import CodecOptions
from collections import OrderedDict
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.operations import UpdateOne, InsertOne

from . import valuechecks
from .analyzerstate import AnalyzerState

Interval = Tuple[datetime, datetime]

FIELDS = {'_id', 'action_id', 'deprecated', 'condition', 'time', 'path', 'analyzer_id', 'sources', 'value'}

codec_opts = CodecOptions(document_class=OrderedDict)


def collection_ensure_order(coll: Collection):
    return coll.with_options(codec_options=codec_opts)


def schema_validator(analyzer_id, action_id: int, timespan: Interval, outputs: Sequence[str]):
    # extra validation needed for:
    # - path: is a list of strings where each string is an IP, IP-prefix, tag or AS-number.
    # - source: is a list of dicts where each dict is a valid source identifier
    # - time.from <= time.to
    return {
        '$and': [
            # complete
            {'analyzer_id':  {'$type': 'int', '$eq': analyzer_id}},
            {'action_id':    {'$type': 'int', '$eq': action_id}},
            {'condition':   {'$type': 'string', '$in': outputs}},
            {'deprecated':  False},

            # incomplete validation
            {'source':      {'$exists': True}},
            {'path':        {'$exists': True}},
            {'$or': [
                {'time':    {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
                {'$and': [
                    {'time.from':   {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
                    {'time.to':     {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
                ]}
            ]}
        ]
    }

pattern_ip4 = re.compile(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")

class ValidationError(Exception):
    def __init__(self, obsid, reason):
        self.obsid = obsid
        self.reason = reason

    def __repr__(self):
        return "Validation Error {}: {}".format(self.obsid, self.reason)


def grouper(iterable, count):
    iterator = iter(iterable)
    while True:
        lst = []
        try:
            for index in range(count):
                lst.append(next(iterator))
        except StopIteration:
            pass

        if len(lst) > 0:
            yield lst
        else:
            break


def check(cond, obsid, reason):
    if not cond:
        raise ValidationError(obsid, reason)


def validate(
        analyzer_id,
        action_id: int,
        timespans: Sequence[Tuple[datetime, datetime]],
        temporary_coll: Collection,
        output_types: Sequence[str],
        abort_max_errors=100):

    errors = []

    # check arguments
    try:
        check(isinstance(action_id, int), None, 'parameter action_id  must be int')

        check(isinstance(timespans, list) and len(timespans) > 0, None, 'no timespans given')

        check(all(len(timespan) == 2 and isinstance(timespan[0], datetime) and
              isinstance(timespan[1], datetime) for timespan in timespans),
              None, 'parameter timespans must be sequence of 2-tuple of datetime')

        check(all(isinstance(output, str) for output in output_types), None, 'parameter output_types must be list of str')
    except ValidationError as e:
        return 0, [(e.obsid, e.reason)]
    except (KeyError, TypeError) as e:
        return 0, [(None, str(e))]

    temporary_ocoll = collection_ensure_order(temporary_coll)

    valid_count = 0
    # TODO optimize
    for doc in temporary_ocoll.find():
        obsid = doc['_id']

        try:
            # check that it has the correct fieldnames
            check(doc.keys() == FIELDS, obsid, 'wrong fields')

            # check that action id is correct
            check(doc['action_id'] == action_id, obsid, 'wrong action id')

            # check that analyzer id is correct
            check(doc['analyzer_id'] == analyzer_id, obsid, 'wrong analyzer id')

            # check that condition is defined in outputs
            condition = doc['condition']
            check(condition in output_types, obsid, 'condition not declared in output_types')

            # check that deprecation value is correct
            check(doc['deprecated'] is False, obsid, 'deprecation setting incorrect')

            # check that time is within any timespan
            time = doc['time']
            if isinstance(time, dict):
                check(any(timespan[0] <= time['from'] <= time['to'] <= timespan[1] for timespan in timespans), obsid, 'timespan')
            else:
                check(any(timespan[0] <= time <= timespan[1] for timespan in timespans), obsid, 'timespan')

            # check that path consists only of valid path elements
            check(isinstance(doc['path'], list), obsid, 'path field is not a list')
            # TODO

            # check that sources exist
            check(isinstance(doc['sources'], list), obsid, 'sources field is not a list')
            # TODO

            # check that value is valid
            check(valuechecks.checks[condition](doc['value']), obsid, 'value')

            valid_count += 1
        except ValidationError as e:
            errors.append((e.obsid, e.reason))

            if len(errors) > abort_max_errors:
                break
        except (KeyError, TypeError) as e:
            errors.append((None, repr(e)))

            if len(errors) > abort_max_errors:
                break

    return valid_count, errors


def find_counterpart(doc, other_coll: Collection):
    """
    Find the document in other_coll which matches doc by the fields
    analyzer_id, condition, path, sources, time and value
    :return: The corresponding document or None if there is no match.
    """

    # Reasons why value is compared in python:
    #
    # 1. Remember in mongodb, to match documents the fields have to be the in the same order.
    # And in python, dictionary is unordered by definition. So either we require each analyzer to
    # insert documents in the same order (ex. sorted) or we perform the equality check in python.
    #
    # 2a.Before single value, now list. equality check is "any item match"
    # 2b.equality check will match if any array item matches.
    #    assume there is doc = {'value': [1, 2, 3]} then query {'value': 2} will match.

    if isinstance(doc['time'], datetime):
        time_check = {'time': doc['time']}
    else:
        time_check = {'$and': [
            {'time.from': doc['time']['from']},
            {'time.to': doc['time']['to']}
        ]}

    query = {
        'analyzer_id': doc['analyzer_id'],
        'condition': doc['condition'],
        'path': doc['path'],
        'sources': doc['sources'],
        'value': doc['value'], #-> perform in python
    }
    query.update(time_check)

    counterparts = other_coll.find(query)

    for counterpart in counterparts:
        if counterpart['value'] == doc['value']:
            return counterpart
    else:
        return None


def commit(analyzer_id: int,
           action_id: int,
           timespans: Sequence[Interval],
           temporary_coll: Collection,
           output_coll: Collection,
           output_types: Sequence[str],
           abort_max_errors=100):

    valid_count, errors = validate(analyzer_id, action_id, timespans, temporary_coll, output_types, abort_max_errors)

    if len(errors) > 0:
        return valid_count, errors

    # TODO let analyzer give us the candidates query. because analyzer knows best which observations to override.
    # TODO for example: special treatment of direct observation analyzers. (additional condition: source)

    def create_timespan_subquery(timespan: Interval):
        return {'$or': [
            {'time': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
            {'$and': [
                {'time.from': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
                {'time.to': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}}
            ]}
        ]}

    # query to find deprecation candidates
    candidates_query = {'analyzer_id': analyzer_id,
                        '$or': [create_timespan_subquery(timespan) for timespan in timespans]}

    # 1. first deprecate all candidates and update action_id
    output_coll.update_many(candidates_query, {'$set': {'deprecated': True, 'action_id': action_id}})

    # 2. find all observations that exist both in the output collection and in the temporary collection
    candidates = output_coll.find(candidates_query)
    pairs = filter(lambda x: x[1] is not None, ((candidate, find_counterpart(candidate, temporary_coll)) for candidate in candidates))

    # 3. mark all of them in the temporary collection
    mark_ops = (UpdateOne({'_id': pair[1]['_id']}, {'$set': {'output_id': pair[0]['_id']}}) for pair in pairs)

    # unfortunately bulk_write does not accept iterators. in the mongodb docs, the server limit is 1000 ops.
    for block in grouper(mark_ops, 1000):
        temporary_coll.bulk_write(list(block))

    # 4. commit changes into output collection
    def create_output_ops():
        # generator that iterates over temporary coll and create operations for output collection
        # note that the find query projection is {'_id': 0}: using this we can simply insert the document
        # into the output collection
        for doc in temporary_coll.find({}, {'_id': 0}):
            if 'output_id' in doc:
                yield UpdateOne({'_id': doc['output_id']}, {'$set': {'deprecated': False}})
            else:
                yield InsertOne(doc)

    for block in grouper(create_output_ops(), 1000):
        output_coll.bulk_write(list(block))

    # 5. finally delete collection
    temporary_coll.drop()

    return valid_count, []

class Validator:
    def __init__(self, analyzers_coll: Collection, analysis_db: Database, output_coll: Collection, loop=None, host='localhost', port=33424):
        self.analyzers_state = AnalyzerState(analyzers_coll)
        self.output_coll = output_coll
        self.analysis_db = analysis_db

    def check_for_work(self):
        executed = self.analyzers_state.executed_analyzers()
        print("validator: check for work")
        for analyzer in executed:
            print("validating and committing {} action id {}".format(analyzer['_id'], analyzer['action_id']))

            self.analyzers_state.transition_to_validating(analyzer['_id'])

            exe_res = analyzer['execution_result']
            temporary_coll = self.analysis_db[exe_res['temporary_coll']]
            valid_count, errors = commit(analyzer['_id'], analyzer['action_id'], exe_res['timespans'],
                                         temporary_coll, self.output_coll, analyzer['output_types'])

            if len(errors) > 0:
                print("analyzer {} with action id {} has at least {} valid records but {} have problems:".format(analyzer['_id'], analyzer['action_id'], valid_count, len(errors)))
                for idx, error in enumerate(errors):
                    print("{}: {}".format(idx, error))

                self.analyzers_state.transition_to_error(analyzer['_id'], 'error when executing validator:\n' + '\n'.join((str(error) for error in errors)))
            else:
                print("successfully commited analyzer {} run with action id {}. {} records inserted".format(analyzer['_id'], analyzer['action_id'], valid_count))
                self.analyzers_state.transition_to_sensing(analyzer['_id'])

    def run(self):
        # TODO consider using threads
        while True:
            self.check_for_work()
            sleep(4)

def main():
    mongo = MongoClient("mongodb://curator:ah8NSAdoITjT49M34VqZL3hEczCHjbcz@localhost/analysis")

    sup = Validator(mongo.analysis.analyzers, mongo.analysis, mongo.analysis.observations)

    sup.run()

if __name__ == "__main__":
    main()