from datetime import datetime, timedelta
from typing import Tuple, Sequence, Callable
from itertools import takewhile

import pymongo
from pymongo.collection import Collection
from bson.objectid import ObjectId

from ptocore.timeline import Timeline

Interval = Tuple[datetime, datetime]


def extend_hourly(interval: Interval) -> Interval:
    start, stop = interval
    assert start <= stop

    start = start.replace(minute=0, second=0, microsecond=0)

    if stop.minute > 0 or stop.second > 0 or stop.microsecond > 0:
        stop = stop.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    return start, stop

class ActionSet:
    def __init__(self,
                 analyzer_id: str,
                 git_url: str,
                 git_commit: str,
                 input_types: Sequence[str],
                 input_formats: Sequence[str],
                 action_log: Collection):

        self.direct_allowed = len(input_types) == 0

        self._load_input_actions(input_types, input_formats, action_log)
        self._load_output_actions(analyzer_id, git_url, git_commit, action_log)

        self.max_action_id = max(self.input_max_action_id, self.output_max_action_id)

    def _load_input_actions(self, input_types, input_formats, action_log):
        query = {
            '$or': [
                {'output_types': {'$in': input_types}},
                {'output_formats': {'$in': input_formats}}
            ]
        }

        result = action_log.find(query, {'timespans': 1}).sort([('_id', pymongo.DESCENDING)])
        self.input_actions = list(result)

        self.input_max_action_id = self.input_actions[0]['_id'] if len(self.input_actions) > 0 else -1

    def _load_output_actions(self, analyzer_id, git_url, git_commit, action_log: Collection):
        query = {'analyzer_id': analyzer_id}
        proj = {'_id': 1, 'git_url': 1, 'git_commit': 1, 'timespans': 1, 'upload_id': 1}

        def same_code(doc):
            return doc['git_url'] == git_url and doc['git_commit'] == git_commit

        result = action_log.find(query, proj).sort([('_id', pymongo.DESCENDING)])
        self.output_actions = list(takewhile(same_code, result))

        # this is the maximum action_id known prior to executing the analyzer. why not just the _id?
        # note that the validator assigns the action_id after execution of the analyzer.
        # because it can happen that between start and finish of the analyzer an upload is added or an upstream
        # analyzer module has finished.
        # therefore the analyzer has to state the maximum action id it has considered.
        self.output_max_action_id = self.output_actions[0]['max_action_id'] if len(self.output_actions) > 0 else -1

    def has_unprocessed_data(self):
        return self.input_max_action_id > self.output_max_action_id

def direct(action_set: ActionSet) -> Sequence[ObjectId]:
    if not action_set.direct_allowed:
        raise ValueError("Cannot use direct sensitivity for basic/dervied analyzer modules."
                         "Check that input_types is an empty list.")

    uploads_processed = set(action['upload_id'] for action in action_set.output_actions)

    uploads_all = (action['upload_id'] for action in action_set.input_actions)

    uploads_unprocessed = [upload for upload in uploads_all if upload not in uploads_processed]

    return action_set.max_action_id, uploads_unprocessed

def _get_timeline(actions):
    tl = Timeline()
    for action in actions:
        for timespan in action['timespans']:
            start, end = timespan
            tl.add_interval(start, end)

    return tl

def basic(action_set: ActionSet) -> Tuple[int, Sequence[Interval]]:
    input_tl = _get_timeline(action_set.input_actions)
    output_tl = _get_timeline(action_set.output_actions)

    return action_set.max_action_id, (input_tl - output_tl).intervals

def aggregating(extend_func: Callable[[Interval], Interval],
                action_set: ActionSet) -> Tuple[int, Sequence[Interval]]:

    max_action_id, timespans = basic(action_set)

    tl = Timeline()
    for timespan in timespans:
        timespan_extended = extend_func(timespan)
        tl.add_interval(timespan_extended[0], timespan_extended[1])

    return max_action_id, tl.intervals


def margin(offset: float,
           action_set: ActionSet):
    pass