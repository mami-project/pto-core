from datetime import datetime
from typing import Sequence, Tuple, Callable
from time import sleep
from hashlib import sha1
import argparse

import re
from bson import CodecOptions
from collections import OrderedDict
from pymongo.errors import BulkWriteError
from pymongo.collection import Collection
from pymongo.operations import UpdateOne, InsertOne

from . import valuechecks
from .analyzerstate import AnalyzerState
from .mongoutils import AutoIncrementFactory
from . import repomanager
from .coreconfig import CoreConfig

Interval = Tuple[datetime, datetime]


VALIDATION_COMPARE_FIELDS = {'condition', 'time', 'path', 'value', 'sources', 'analyzer_id'}

VALIDATION_INPUT_FIELDS = VALIDATION_COMPARE_FIELDS | {'_id'}

VALIDATION_OUTPUT_FIELDS = VALIDATION_COMPARE_FIELDS | {'action_id', 'valid'}

COMPARE_PROJECTION = {'_id': 0, 'condition': 1, 'path': 1, 'analyzer_id': 1, 'sources': 1, 'value': 1}

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
            {'condition':   {'$type': 'string', '$in': outputs}},
            #{'valid':  True},
            # TODO tell schema mongodb schema validator action_id don't care if exist or not
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
    def __init__(self, obsid, reason: str, extra: str=''):
        self.obsid = obsid
        self.reason = reason
        self.extra = extra

    def __repr__(self):
        return "Validation Error {}: {} {}".format(self.obsid, self.reason, self.extra)


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

def grouper_transpose(iterable, count, tuple_length=2):
    for group in grouper(iterable, count):
        newl = []
        for n in range(tuple_length):
            newl.append([tup[n] for tup in group])
        yield newl


def dict_to_sorted_list(obj):
    if isinstance(obj, dict):
        return [dict_to_sorted_list([key, obj[key]]) for key in sorted(obj.keys())]
    elif isinstance(obj, list):
        return [dict_to_sorted_list(elem) for elem in obj]
    else:
        return obj


def rflatten(obj):
    out = []
    for elem in obj:
        if isinstance(elem, list):
            out.append('[')
            out.extend(rflatten(elem))
            out.append(']')
        else:
            out.append(elem)
    return out


def create_hash(obs):
    hs = sha1()

    cmp = {key: value for key, value in obs.items() if key in VALIDATION_COMPARE_FIELDS}

    for elem in rflatten(dict_to_sorted_list(cmp)):
        hs.update(str(elem).encode('utf-8'))

    return hs.digest()


def equal_observation(a, b):
    return all(a[key] == b[key] for key in VALIDATION_COMPARE_FIELDS)


def find_counterpart(candidate, temporary_coll):
    hash = create_hash(candidate)

    for counterpart in temporary_coll.find({'hash': hash}):
        if equal_observation(candidate, counterpart):
            return candidate, counterpart

    return None


def check(cond, obsid, reason: str, extra: str=''):
    if not cond:
        raise ValidationError(obsid, reason, extra)


def validate(
        analyzer_id,
        timespans: Sequence[Tuple[datetime, datetime]],
        temporary_coll: Collection,
        output_types: Sequence[str],
        abort_max_errors=100):

    errors = []

    # check arguments
    try:
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
            check(doc.keys() == VALIDATION_INPUT_FIELDS, obsid, 'wrong fields', 'expected {}, got {}'.format(VALIDATION_INPUT_FIELDS, doc.keys()))

            # check that analyzer id is correct
            check(doc['analyzer_id'] == analyzer_id, obsid, 'wrong analyzer id',
                  'expected {}, got {}'.format(analyzer_id, doc['analyzer_id']))

            # check that condition is defined in outputs
            condition = doc['condition']
            check(condition in output_types, obsid,
                  'condition not declared in output_types', 'expected one of {}, got {}'.format(output_types, condition))

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
            errors.append((e.obsid, e.reason, e.extra))

            if len(errors) > abort_max_errors:
                break
        except (KeyError, TypeError) as e:
            errors.append((None, str(e), repr(e)))

            if len(errors) > abort_max_errors:
                break

    return valid_count, errors


def commit(analyzer_id: int,
           repo_path: str,
           action_id_creator: Callable[[], int],
           timespans: Sequence[Interval],
           temporary_coll: Collection,
           output_coll: Collection,
           output_types: Sequence[str],
           action_log: Collection,
           abort_max_errors=100):

    # repository is cleaned by supervisor prior to analyzer module execution
    try:
        git_commit = repomanager.get_repository_commit(repo_path)
        git_url = repomanager.get_repository_url(repo_path)
    except repomanager.RepositoryError as e:
        raise ValidationError(None, "either working_dir is not pointing to a git repository"
                              " or it's not possible to obtain commit and git url.",
                              "analyzer: '{}', working_dir: '{}'.".format(analyzer_id, repo_path)) from e

    print("a. validating.")
    valid_count, errors = validate(analyzer_id, timespans, temporary_coll, output_types, abort_max_errors)

    if len(errors) > 0:
        return valid_count, errors, 0

    # create and set action_id
    action_id = action_id_creator()

    if not isinstance(action_id, int) or action_id < 0:
        return 0, [(None, "action id has to be a non-negative integer.")], action_id

    temporary_coll.update_many({}, {'$set': {'action_id': action_id, 'valid': True}})

    # TODO let analyzer give us the candidates query. because analyzer knows best which observations to override.
    # TODO for example: special treatment of direct observation analyzers. (additional condition: source)

    print("b. determine candidates to invalidate")
    def create_timespan_subquery(timespan: Interval):
        return {'$or': [
            {'time': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
            {'$and': [
                {'time.from': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
                {'time.to': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}}
            ]}
        ]}

    # query to find candidates to invalidate
    candidates_query = {'analyzer_id': analyzer_id,
                        '$or': [create_timespan_subquery(timespan) for timespan in timespans]}

    # create hashes
    for obs_group in grouper(temporary_coll.find(), 1000):
        bulk = [UpdateOne({'_id': obs['_id']}, {'$set': {'hash': create_hash(obs)}}) for obs in obs_group]
        temporary_coll.bulk_write(bulk)

    temporary_coll.create_index('hash')

    # 1. first set all candidates to invalid and update their action_id.
    # TODO should action_id be an array of tuple (action_id, True/False)
    output_coll.update_many(candidates_query, {'$set': {'valid': False, 'action_id': action_id}})

    print("c. find candidates")
    # 2. find all observations that exist both in the output collection and in the temporary collection
    candidates = output_coll.find(candidates_query)

    print("d. find counterparts and mark them")

    pairs = filter(None, (find_counterpart(candidate, temporary_coll) for candidate in candidates))

    # 3. mark all of them in the temporary collection, they will be set to valid again
    mark_ops = (UpdateOne({'_id': pair[1]['_id']}, {'$set': {'output_id': pair[0]['_id']}}) for pair in pairs)

    # unfortunately bulk_write does not accept iterators. in the mongodb docs, the server limit is 1000 ops.
    print("blapp")
    for block in grouper(mark_ops, 1000):
        print("blupp")
        temporary_coll.bulk_write(list(block))

    print("e. insert new or validate existing observations.")

    # 4. commit changes into output collection.
    def create_output_ops():
        kept = 0
        inserted = 0
        # generator that iterates over temporary coll and create operations for output collection
        # note that the find query projection is {'_id': 0}: using this we can simply insert the document
        # into the output collection
        for doc in temporary_coll.find({}, {'_id': 0}):
            if 'output_id' in doc:
                yield UpdateOne({'_id': doc['output_id']}, {'$set': {'valid': True}})
                kept+=1
            else:
                yield InsertOne(doc)
                inserted+=1

        print("commit stats: {} kept, {} inserted".format(kept, inserted))

    # action log
    action_log.insert_one({
        '_id': action_id,
        'output_types': output_types,
        'timespans': timespans,
        'action': 'analyze',
        'analyzer_id': analyzer_id,
        'git_url': git_url,
        'git_commit': git_commit
    })

    print("perform critical write")
    for block in grouper(create_output_ops(), 1000):
        output_coll.bulk_write(list(block))

    print("f. done. drop temporary collection")
    # 5. finally delete collection
    temporary_coll.drop()

    return valid_count, [], action_id

class Validator:
    def __init__(self, core_config: CoreConfig):
        self.core_config = core_config
        self.analyzer_state = AnalyzerState('validator', core_config.analyzers_coll)

        # the validator is the only component generating action_ids, therefore create_if_missing=True is not a problem.
        idfactory = AutoIncrementFactory(core_config.idfactory_coll)
        self._action_id_creator = idfactory.get_incrementor('action_id', create_if_missing=True)

    def check_for_uploads(self):
        """
        Assign action_id to every completed upload
        """

        cc = self.core_config

        action_id_field = 'action_id.'+cc.environment
        def set_action_id_ops() -> Sequence[Tuple[UpdateOne, InsertOne]]:
            find_query = {
                'complete': True,
                'valid': True,
                action_id_field: {'$exists': False},
                'meta.format': {'$exists': True},
                'meta.start_time': {'$exists': True},
                'meta.stop_time': {'$exists': True}
            }

            cursor = cc.metadata_coll.find(find_query).sort('timestamp')
            for upload in cursor:
                action_id = self._action_id_creator()
                print("assign action id {} to upload {}".format(action_id, upload['_id']))
                uploads_query = UpdateOne({'_id': upload['_id']}, {'$set': {action_id_field: action_id}})

                timespans = [(upload['meta']['start_time'], upload['meta']['stop_time'])]

                action_log_query = InsertOne({
                    '_id': action_id,
                    'output_formats': [upload['meta']['format']],
                    'timespans': timespans,
                    'action': 'upload',
                    'upload_id': upload['_id']
                })

                yield uploads_query, action_log_query

        try:
            for uploads_block, action_log_block in grouper_transpose(set_action_id_ops(), 1000):
                cc.metadata_coll.bulk_write(uploads_block)
                cc.action_log.bulk_write(action_log_block)
        except BulkWriteError as e:
            # most likely a configuration error
            print(e.details)
            raise


    def check_for_analyzers(self):
        cc = self.core_config
        executed = self.analyzer_state.executed_analyzers()
        for analyzer in executed:
            # check for wish
            if self.analyzer_state.check_wish(analyzer, 'cancel'):
                print("validator: cancelled {} upon request".format(analyzer['_id']))
                continue

            print("validating and committing {}".format(analyzer['_id']))

            self.analyzer_state.transition(analyzer['_id'], 'executed', 'validating')

            exe_res = analyzer['execution_result']
            temporary_coll = cc.temporary_db[exe_res['temporary_coll']]
            valid_count, errors, action_id = commit(analyzer['_id'], analyzer['working_dir'], self._action_id_creator,
                                                    exe_res['timespans'], temporary_coll, cc.observations_coll,
                                                    analyzer['output_types'], cc.action_log)

            if len(errors) > 0:
                print("analyzer {} with action id {} has at least {} valid records but {} have problems:".format(analyzer['_id'], action_id, valid_count, len(errors)))
                for idx, error in enumerate(errors):
                    print("{}: {}".format(idx, error))

                self.analyzer_state.transition_to_error(analyzer['_id'], 'error when executing validator:\n' + '\n'.join((str(error) for error in errors)))
            else:
                print("successfully commited analyzer {} run with action id {}. {} records inserted".format(analyzer['_id'], action_id, valid_count))
                self.analyzer_state.transition(analyzer['_id'], 'validating', 'sensing', {'action_id': action_id})

    def check_for_work(self):
        print("validator: check for work")
        self.check_for_analyzers()
        self.check_for_uploads()


def main():
    desc = 'Monitor the observatory for changes and order execution of analyzer modules.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('config_file', type=argparse.FileType('rt'))
    args = parser.parse_args()

    cc = CoreConfig('validator', args.config_file)

    val = Validator(cc)

    while True:
        val.check_for_work()
        sleep(4)

if __name__ == "__main__":
    main()