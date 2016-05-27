from datetime import datetime
from typing import Sequence, Tuple

import re
from bson import CodecOptions
from collections import OrderedDict
from pymongo.collection import Collection
from pymongo.operations import UpdateOne, InsertOne

from . import valuechecks

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


def check(cond, obsid, reason):
    if not cond:
        raise ValidationError(obsid, reason)


def validate(
        analyzer_id: int,
        action_id: int,
        timespan: Tuple[datetime, datetime],
        temporary_coll: Collection,
        outputs: Sequence[str],
        abort_max_errors=100):

    errors = []

    # check arguments
    try:
        check(isinstance(analyzer_id, int), None, 'param analyzer_id must be int')
        check(isinstance(action_id, int), None, 'parameter action_id  must be int')
        check(len(timespan) == 2 and isinstance(timespan[0], datetime) and
              isinstance(timespan[1], datetime), None, 'parameter timespan must be 2-tuple of datetime')
        check(all(isinstance(output, str) for output in outputs), None, 'parameter outputs must be list of str')
    except ValidationError as e:
        return 0, [(e.obsid, e.reason)]

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
            check(condition in outputs, obsid, 'condition not declared')

            # check that deprecation value is correct
            check(doc['deprecated'] is False, obsid, 'deprecation setting incorrect')

            # check that time is within timespan
            time = doc['time']
            if isinstance(time, dict):
                check(timespan[0] <= time['from'] <= time['to'] <= timespan[1], obsid, 'timespan')
            else:
                check(timespan[0] <= time <= timespan[1], obsid, 'time')

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

    counterparts = other_coll.find({
        'analyzer_id': doc['analyzer_id'],
        'condition': doc['condition'],
        'path': doc['path'],
        'sources': doc['sources'],
        #'value': doc['value'], -> perform in python
        '$or': [
            {'time': doc['time']},
            {'$and': [
                {'time.from': doc['time']['from']},
                {'time.to': doc['time']['to']}
            ]}
        ]
    })

    for counterpart in counterparts:
        if counterpart['value'] == doc['value']:
            return counterpart
    else:
        return None

def commit(analyzer_id: int,
           action_id: int,
           timespan: Tuple[datetime, datetime],
           temporary_coll: Collection,
           output_coll: Collection,
           outputs: Sequence[str],
           abort_max_errors=100):

    errors = validate(analyzer_id, action_id, timespan, temporary_coll, outputs, abort_max_errors)

    if len(errors) > 0:
        return errors

    # TODO let analyzer give us the candidates query. because analyzer knows best which observations to override.
    # TODO for example: special treatment of direct observation analyzers. (additional condition: source)

    # query to find deprecation candidates
    candidates_query = {'analyzer_id': analyzer_id,
                        '$or': [
                        {'time':    {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
                            {'$and': [
                            {'time.from':   {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
                            {'time.to':     {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
                            ]}
                        ]}

    # 1. first deprecate all candidates and update action_id
    output_coll.update_many(candidates_query, {'deprecated': True, 'action_id': action_id})

    # 2. find all observations that exist both in the output collection and in the temporary collection
    candidates = output_coll.find(candidates_query)
    pairs = ((candidate, find_counterpart(candidate, temporary_coll)) for candidate in candidates)

    # 3. mark all of them in the temporary collection
    mark_ops = (UpdateOne({'_id': pair[1]['_id']}, {'output_id': pair[0]['_id']}) for pair in pairs)
    temporary_coll.bulk_write(mark_ops)

    # 4. commit changes into output collection
    def create_output_ops():
        # generator that iterates over temporary coll and create operations for output collection
        # note that the find query projection is {'_id': 0}: using this we can simply insert the document
        # into the output collection
        for doc in temporary_coll.find({}, {'_id': 0}):
            if 'output_id' in doc:
                yield UpdateOne({'_id': doc['output_id']}, {'deprecated': False})
            else:
                yield InsertOne(doc)

    output_coll.bulk_write(create_output_ops())

    # 5. finally delete collection
    temporary_coll.drop()
