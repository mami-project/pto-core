from datetime import datetime
from typing import Sequence, Tuple, Callable
from hashlib import sha1
import logging

from pymongo.collection import Collection
from pymongo.operations import UpdateOne, InsertOne
from bson.objectid import ObjectId

from .collutils import grouper, rflatten, dict_to_sorted_list
from . import repomanager
from .validation import ValidationError, validate, VALIDATION_COMPARE_FIELDS

Interval = Tuple[datetime, datetime]


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


def get_repo_info(self, analyzer_id, repo_path):
    try:
        git_commit = repomanager.get_repository_commit(repo_path)
        git_url = repomanager.get_repository_url(repo_path)
        return git_url, git_commit
    except repomanager.RepositoryError as e:
        raise ValidationError(None, "either working_dir is not pointing to a git repository"
                              " or it's not possible to obtain commit and git url.",
                              "analyzer: '{}', working_dir: '{}'.".format(analyzer_id, repo_path)) from e


def compute_hashes(coll: Collection):
    for obs_group in grouper(coll.find(), 1000):
        bulk = [UpdateOne({'_id': obs['_id']}, {'$set': {'hash': create_hash(obs)}}) for obs in obs_group]
        coll.bulk_write(bulk)

    coll.create_index('hash')


def perform_commit(analyzer_id: str,
                   output_types: Sequence[str],
                   timespans,
                   upload_ids,
                   max_action_id: int,
                   git_url: str,
                   git_commit: str,
                   temporary_coll: Collection,
                   output_coll: Collection,
                   candidates_query: dict,
                   action_log: Collection,
                   action_id: int):

    # 1. create action_log entry.
    # note the sensor will not run downstream analyzers as long as this analyzer is in validating state.
    action_log.insert_one({
        '_id': action_id,
        'action': 'analyze',
        'output_types': output_types,
        'timespans': timespans,
        'upload_ids': upload_ids,
        'max_action_id': max_action_id,
        'analyzer_id': analyzer_id,
        'git_url': git_url,
        'git_commit': git_commit
    })

    # 2. create hashes for counterpart search
    compute_hashes(temporary_coll)

    # 3. find all observations that exist both in the output collection and in the temporary collection
    print("2. find candidates")
    candidates = output_coll.find(candidates_query)

    # 4. find and mark all of them in the temporary collection, they will be set to valid again
    print("3. find counterparts and mark them")
    pairs = filter(None, (find_counterpart(candidate, temporary_coll) for candidate in candidates))

    mark_ops = (UpdateOne({'_id': pair[1]['_id']}, {'$set': {'output_id': pair[0]['_id']}}) for pair in pairs)

    # unfortunately bulk_write does not accept iterators. in the mongodb docs, the server limit is 1000 ops.
    for block in grouper(mark_ops, 1000):
        print(".")
        temporary_coll.bulk_write(list(block))

    #
    # WRITE TO OBSERVATIONS COLLECTION STARTS FROM HERE
    #

    # 5. push a new action_id and valid: False to all candidates that were valid before.
    # later in the code this item is removed iff the candidate is still valid.
    candidates_query_valid = candidates_query.copy()
    candidates_query_valid.update({
        'action_ids.0.valid': True
    })

    num_marked_false = output_coll.update_many(candidates_query_valid, {
        '$push': {'action_ids': {
            '$each': [{'id': action_id, 'valid': False}],
            '$position': 0
        }}
    }).modified_count

    print("marked false: {}".format(num_marked_false))

    # 6. perform actual commit
    print("e. insert new or validate existing observations.")
    def create_output_ops():
        kept = 0
        inserted = 0
        # generator that iterates over temporary coll and create operations for output collection
        # note that the find query projection is {'_id': 0}: using this we can simply insert the document
        # into the output collection
        for doc in temporary_coll.find({}, {'_id': 0}):
            if 'output_id' in doc:
                # if observation was valid before, pop the current item because validation status hasn't changed
                yield UpdateOne({'_id': doc['output_id'], 'action_ids.0.id': action_id, 'action_ids.1.valid': True},
                                {'$pop': {'action_ids': -1}})

                # if observation was invalid before, push a valid item
                yield UpdateOne({'_id': doc['output_id'], 'action_ids.0.valid': False},
                                {'$push': {
                                    'action_ids': {'$each': [{'id': action_id, 'valid': True}], '$position': 0}
                                }})

                kept+=1
            else:
                yield InsertOne(doc)
                inserted+=1

        deprecated = max(num_marked_false - kept, 0)

        print("commit stats: deprecated(+)/undeprecated(-): {}, kept {}, added: {}".format(deprecated, kept, inserted))

    for block in grouper(create_output_ops(), 1000):
        output_coll.bulk_write(list(block))


def action_ids_timespans_from_uploads(upload_ids: Sequence[ObjectId], action_log: Collection) -> Tuple[Sequence[int], Sequence[Interval]]:
    timespans = []
    action_ids = []
    for upload_id in upload_ids:
        action_doc = action_log.find_one({'action': 'upload', 'upload_ids': [upload_id]})
        if action_doc is None:
            raise ValidationError(None, "cannot find the action_id of given upload_id", repr(upload_id))

        timespans.append(action_doc['timespans'][0])
        action_ids.append(action_doc['_id'])
    return action_ids, timespans


def commit_direct(analyzer_id: str,
                  repo_path: str,
                  action_id_creator: Callable[[], int],
                  upload_ids: Sequence[ObjectId],
                  max_action_id: int,
                  temporary_coll: Collection,
                  output_coll: Collection,
                  output_types: Sequence[str],
                  action_log: Collection,
                  abort_max_errors=100):
    # get repository details
    try:
        git_url, git_commit = repomanager.get_repository_url_commit(repo_path)
    except repomanager.RepositoryError as e:
        raise ValidationError(None, "either working_dir is not pointing to a git repository"
                              " or it's not possible to obtain commit and git url.",
                              "analyzer: '{}', working_dir: '{}'.".format(analyzer_id, repo_path)) from e

    # get action id for each upload
    upload_action_ids, upload_timespans = action_ids_timespans_from_uploads(upload_ids, action_log)

    print("a. validating.")
    valid_count, errors = validate(analyzer_id, upload_timespans, temporary_coll, output_types, abort_max_errors)

    if len(errors) > 0:
        return valid_count, errors, 0

    # TODO: think about if this is reasonable
    # TODO: but if the analyzer module removes input_formats we may never invalidate uploads with the removed input format
    # TODO: maybe write a script that periodically scans for these issues
    candidates_query = {'analyzer_id': analyzer_id, 'sources.upl': {'$in': upload_action_ids}}

    # create and set action_id
    action_id = action_id_creator()

    temporary_coll.update_many({}, {'$set': {'analyzer_id': analyzer_id, 'action_ids': [{'id': action_id, 'valid': True}]}})

    perform_commit(analyzer_id, output_types, upload_timespans, upload_ids, max_action_id, git_url, git_commit,
                          temporary_coll, output_coll, candidates_query, action_log, action_id)

    print("f. done. drop temporary collection")
    # 5. finally delete collection
    temporary_coll.drop()

    return valid_count, [], action_id


def commit_normal(analyzer_id: str,
           repo_path: str,
           action_id_creator: Callable[[], int],
           timespans: Sequence[Interval],
           max_action_id: int,
           temporary_coll: Collection,
           output_coll: Collection,
           output_types: Sequence[str],
           action_log: Collection,
           abort_max_errors=100):

    # get repository details
    try:
        git_url, git_commit = repomanager.get_repository_url_commit(repo_path)
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

    temporary_coll.update_many({}, {'$set': {'analyzer_id': analyzer_id, 'action_ids': [{'id': action_id, 'valid': True}]}})

    # query to find candidates to invalidate
    print("b. determine candidates to invalidate")
    def create_timespan_subquery(timespan: Interval):
        return {'$or': [
            {'time': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
            {'$and': [
                {'time.from': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
                {'time.to': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}}
            ]}
        ]}

    candidates_query = {'analyzer_id': analyzer_id, '$or': [create_timespan_subquery(timespan) for timespan in timespans]}

    perform_commit(analyzer_id, output_types, timespans, None, max_action_id, git_url, git_commit,
                   temporary_coll, output_coll, candidates_query, action_log, action_id)

    print("f. done. drop temporary collection")
    # 5. finally delete collection
    temporary_coll.drop()

    return valid_count, [], action_id
