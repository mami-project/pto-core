from datetime import datetime
from typing import Sequence, Tuple, Callable
from hashlib import sha1

from pymongo.collection import Collection
from pymongo.operations import UpdateOne, InsertOne

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


def perform_commit(temporary_coll: Collection,
                   output_coll: Collection,
                   candidates_query: dict,
                   action_id: int):
    print("c. find candidates")
    # 2. find all observations that exist both in the output collection and in the temporary collection
    candidates = output_coll.find(candidates_query)

    print("d. find counterparts and mark them")

    pairs = filter(None, (find_counterpart(candidate, temporary_coll) for candidate in candidates))

    # 3. mark all of them in the temporary collection, they will be set to valid again
    mark_ops = (UpdateOne({'_id': pair[1]['_id']}, {'$set': {'output_id': pair[0]['_id']}}) for pair in pairs)

    # unfortunately bulk_write does not accept iterators. in the mongodb docs, the server limit is 1000 ops.
    for block in grouper(mark_ops, 1000):
        print(".")
        temporary_coll.bulk_write(list(block))

    # push a new action_id and valid: False to all candidates that were valid before.
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

    print("perform critical write")
    for block in grouper(create_output_ops(), 1000):
        output_coll.bulk_write(list(block))


def candidates_query_timespans(analyzer_id, timespans):
    def create_timespan_subquery(timespan: Interval):
        return {'$or': [
            {'time': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
            {'$and': [
                {'time.from': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}},
                {'time.to': {'$type': 'date', '$gte': timespan[0], '$lte': timespan[1]}}
            ]}
        ]}

    # query to find candidates to invalidate
    return {'analyzer_id': analyzer_id, '$or': [create_timespan_subquery(timespan) for timespan in timespans]}


def commit_direct(analyzer_id: str,
                  repo_path: str,
                  action_id_creator: Callable[[], int],
                  upload_action_id: int,
                  temporary_coll: Collection,
                  output_coll: Collection,
                  output_types: Sequence[str],
                  action_log: Collection,
                  abort_max_errors=100):

    upload_action_doc = action_log.find_one({'_id': upload_action_id})

    candidates_query = {'analyzer_id': analyzer_id, 'sources': [upload_action_id]}

    timespans = [
        (upload_action_doc['meta']['start_time'], upload_action_doc['meta']['stop_time'])
    ]

    max_action_id = upload_action_id

    # zuerst sensor neu basteln

    return commit_base(analyzer_id, repo_path, action_id_creator, timespans, max_action_id,
                       temporary_coll, output_coll, output_types, action_log, abort_max_errors)


def commit_base(analyzer_id: str,
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


def commit_derived(analyzer_id: str,
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

    if not isinstance(action_id, int) or action_id < 0:
        return 0, [(None, "action id has to be a non-negative integer.")], action_id

    temporary_coll.update_many({}, {'$set': {'action_ids': [{'id': action_id, 'valid': True}]}})

    # TODO let analyzer give us the candidates query. because analyzer knows best which observations to override.
    # TODO for example: special treatment of direct observation analyzers. (additional condition: source)

    print("b. determine candidates to invalidate")
    candidates_query = candidates_query_timespans(analyzer_id, timespans)

    # create hashes
    compute_hashes(temporary_coll)

    # action log
    action_log.insert_one({
        '_id': action_id,
        'output_types': output_types,
        'timespans': timespans,
        'max_action_id': max_action_id,
        'action': 'analyze',
        'analyzer_id': analyzer_id,
        'git_url': git_url,
        'git_commit': git_commit
        # upload_id for direct analyzers
    })

    perform_commit(temporary_coll, output_coll, candidates_query, action_id)

    print("f. done. drop temporary collection")
    # 5. finally delete collection
    temporary_coll.drop()

    return valid_count, [], action_id