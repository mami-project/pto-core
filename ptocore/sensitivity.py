from datetime import datetime, timedelta
from typing import Tuple, Sequence, Callable
from itertools import takewhile
from collections import OrderedDict

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


class ActionSetBase:
    def __init__(self,
                 input_formats: Sequence[str],
                 input_types: Sequence[str]):
        self._input_types = input_types
        self._input_formats = input_formats

        self.input_actions = []
        self.input_max_action_id = -1
        self.output_actions = []
        self.output_max_action_id = -1

    def is_direct_allowed(self):
        return len(self._input_types) == 0

    def get_max_action_id(self):
        return max(self.input_max_action_id, self.output_max_action_id)

    def has_unprocessed_data(self):
        return self.input_max_action_id > self.output_max_action_id


class ActionSetTest(ActionSetBase):
    def __init__(self,
                 input_actions: Sequence[dict],
                 output_actions: Sequence[dict],
                 input_formats: Sequence[str],
                 input_types: Sequence[str]):

        super().__init__(input_formats, input_types)
        self.input_actions = input_actions
        if len(input_actions) > 0:
            self.input_max_action_id = max(action['_id'] for action in input_actions)
        else:
            self.input_max_action_id = -1

        self.output_actions = output_actions
        if len(output_actions) > 0:
            self.output_max_action_id = max(action['_id'] for action in output_actions)
        else:
            self.output_max_action_id = -1

class ActionSetMongo(ActionSetBase):
    def __init__(self,
                 analyzer_id: str,
                 git_url: str,
                 git_commit: str,
                 input_formats: Sequence[str],
                 input_types: Sequence[str],
                 action_log: Collection):

        super().__init__(input_formats, input_types)

        self._load_input_actions(input_types, input_formats, action_log)
        self._load_output_actions(analyzer_id, git_url, git_commit, action_log)

    def _load_input_actions(self, input_types, input_formats, action_log):
        query = {
            '$or': [
                {'output_types': {'$in': input_types}},
                {'output_formats': {'$in': input_formats}}
            ]
        }

        result = action_log.find(query, {'timespans': 1, 'upload_ids': 1}).sort([('_id', pymongo.DESCENDING)])
        self.input_actions = list(result)

        self.input_max_action_id = self.input_actions[0]['_id'] if len(self.input_actions) > 0 else -1

    def _load_output_actions(self, analyzer_id, git_url, git_commit, action_log: Collection):
        query = {'analyzer_id': analyzer_id}
        proj = {'_id': 1, 'git_url': 1, 'git_commit': 1, 'timespans': 1, 'upload_ids': 1}

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


def direct(action_set: ActionSetBase) -> Sequence[Tuple[ObjectId, int]]:
    if not action_set.is_direct_allowed():
        raise ValueError("Cannot use direct sensitivity for basic/dervied analyzer modules."
                         "Check that input_types is an empty list.")

    # TODO what to do with marked_valid and marked_invalid?
    # a) The validator itself can do that without invoking the analyzer. -> a lot of code changes :-(
    # b) 1. for each upload_id discard all input actions except the one with the highest _id
    #    2. discard each output action that is smaller than the max input action
    # c) for each upload_id determine if it has been analyzed:
    #   -
    #
    # Upon change of analyzer code, all previous invocations of analyzer module don't show up anymore in output_actions already.
    #

    # get the maximum action_id (last change) and minimum action_id (upload) for each upload
    uploads_max_action_id = OrderedDict()
    uploads_min_action_id = OrderedDict()
    for action in action_set.input_actions:
        # the list upload has exactly one item
        uid = action['upload_ids'][0]
        aid = action['_id']
        if uploads_max_action_id.get(uid, -1) < aid:
            uploads_max_action_id[uid] = aid

        if action['action'] == 'upload':
            uploads_min_action_id[uid] = aid

    # for each upload determine if it has been analyzed
    uploads_processed = []
    for upload_id, upload_max_action_id in uploads_max_action_id.items():
        for analysis in action_set.output_actions:
            if analysis['_id'] > upload_max_action_id and upload_id in analysis['upload_ids']:
                uploads_processed.append(upload_id)
                break

    uploads_unprocessed = [upload_id for upload_id in uploads_max_action_id if upload_id not in uploads_processed]

    return action_set.get_max_action_id(), uploads_unprocessed


def _get_timeline(actions):
    tl = Timeline()
    for action in actions:
        for timespan in action['timespans']:
            start, end = timespan
            tl.add_interval(start, end)

    return tl


def basic(action_set: ActionSetBase) -> Tuple[int, Sequence[Interval]]:
    input_tl = _get_timeline(action_set.input_actions)
    output_tl = _get_timeline(action_set.output_actions)

    return action_set.get_max_action_id(), (input_tl - output_tl).intervals


def aggregating(extend_func: Callable[[Interval], Interval],
                action_set: ActionSetBase) -> Tuple[int, Sequence[Interval]]:

    max_action_id, timespans = basic(action_set)

    tl = Timeline()
    for timespan in timespans:
        timespan_extended = extend_func(timespan)
        tl.add_interval(timespan_extended[0], timespan_extended[1])

    return max_action_id, tl.intervals


def margin(offset: float,
           action_set: ActionSetBase):
    pass