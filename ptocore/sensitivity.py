from datetime import datetime, timedelta
from typing import Tuple, Sequence, Callable
from itertools import takewhile
from collections import OrderedDict

import pymongo
from pymongo.collection import Collection
from bson.objectid import ObjectId
from . import timeline

Interval = Tuple[datetime, datetime]


def extend_hourly(interval: Interval) -> Interval:
    start, stop = interval
    assert start <= stop

    start = start.replace(minute=0, second=0, microsecond=0)

    if stop.minute > 0 or stop.second > 0 or stop.microsecond > 0:
        stop = stop.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    return start, stop

def _get_timeline(actions):
    tl = timeline.Timeline()
    for action in actions:
        for timespan in action['timespans']:
            start, end = timespan
            tl.add_interval(start, end)

    return tl

class ActionSetBase:
    def __init__(self,
                 input_formats: Sequence[str],
                 input_types: Sequence[str]):
        self._input_types = input_types
        self._input_formats = input_formats

        # input_actions fields: _id, action, upload_ids, timespans
        self.input_actions = []
        self.input_max_action_id = -1

        # output_actions fields: _id, upload_ids, timespans
        self.output_actions = []
        self.output_max_action_id = -1

    def is_direct_allowed(self):
        return len(self._input_types) == 0

    def get_max_action_id(self):
        return max(self.input_max_action_id, self.output_max_action_id)

    def has_unprocessed_data(self, is_direct: bool) -> bool:
        if is_direct:
            max_action_id, uploads_unprocessed = self.direct()
            return len(uploads_unprocessed) > 0
        else:
            max_action_id, timespans = self.basic()
            return len(timespans) > 0

    def direct(self) -> Sequence[Tuple[ObjectId, int]]:
        if not self.is_direct_allowed():
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
        for action in self.input_actions:
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
            for analysis in self.output_actions:
                if analysis['max_action_id'] >= upload_max_action_id and upload_id in analysis['upload_ids']:
                    uploads_processed.append(upload_id)
                    break

        uploads_unprocessed = [upload_id for upload_id in uploads_max_action_id if upload_id not in uploads_processed]

        return self.get_max_action_id(), uploads_unprocessed

    def basic(self) -> Tuple[int, Sequence[Interval]]:

        unsorted_actions = []
        for action in self.input_actions:
            unsorted_actions.append((True, action))

        for action in self.output_actions:
            unsorted_actions.append((False, action))

        sorted_actions = sorted(unsorted_actions, key=lambda action: action[1]['_id'])

        todo_tl = timeline.Timeline()
        for to_add, action in sorted_actions:
            for timespan in action['timespans']:
                start, end = timespan
                if to_add:
                    todo_tl.add_interval(start, end)
                else:
                    todo_tl.remove_interval(start, end)

        return self.get_max_action_id(), todo_tl.intervals

    def input_timespans(self) -> Tuple[int, Sequence[Interval]]:
        """
        Return the max action id and merged timespans of the input actions
        """
        
        input_tl = timeline.Timeline()
        for action in self.input_actions:
            for timespan in action['timespans']:
                start, end = timespan
                input_tl.add_interval(start, end)
            
        return self.get_max_action_id(), input_tl.intervals

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
            self.output_max_action_id = max(action['max_action_id'] for action in output_actions)
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

        result = action_log.find(query, {'action': 1, 'timespans': 1, 'upload_ids': 1}).sort([('_id', pymongo.DESCENDING)])
        self.input_actions = list(result)

        self.input_max_action_id = self.input_actions[0]['_id'] if len(self.input_actions) > 0 else -1

    def _load_output_actions(self, analyzer_id, git_url, git_commit, action_log: Collection):
        query = {'analyzer_id': analyzer_id}
        proj = {'_id': 1, 'git_url': 1, 'git_commit': 1, 'timespans': 1, 'upload_ids': 1, 'max_action_id': 1}

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


def extend(extend_func: Callable[[Interval], Interval],
                action_set: ActionSetBase) -> Tuple[int, Sequence[Interval]]:

    max_action_id, timespans = action_set.basic()

    tl = timeline.Timeline()
    for timespan in timespans:
        timespan_extended = extend_func(timespan)
        tl.add_interval(timespan_extended[0], timespan_extended[1])

    return max_action_id, tl.intervals

def get_islands(islands, input_timespans):
    """
    Return the islands that contain elements from input_timespans
    
    Given two lists of timespans: `islands` and `input_timespans`,
    return all elements of `islands` that overlap with one or more
    elements of `intput_timespans`
    """

    output_timespans = set()

    # For every input timespan, we check with wat islands it overlaps
    for input_timespan in input_timespans:
        input_start, input_end = input_timespan
        for island in islands:
            island_start, island_end = island

            # If the input timespan starts before the start of island, and the
            # input timespan end after the start of the island.
            # In other words: The island starts during the input timespan
            # Examples:
            #                    input timespan start
            #                    |
            #                    |   island start
            #                    |   \
            # island:            \    =======           OR      ======
            # input_timespan:     ======     \                ===========
            #                           \    island end
            #                            |
            #                            input timespand end
            #
            if input_start <= island_start and input_end >= island_start:
                output_timespans.add(island)
                continue

            # If the input timespan starts after the start of the island, and
            # the input timespand ends before the end of the island.
            # In other words: The input timespan starts during the island
            #                       
            # island:           =======           OR         ==========
            # input_timespan:        ======                     =====
            #
            if input_start >= island_start and input_start <= island_end:
                output_timespans.add(island)
                continue

    return output_timespans

def margin(offset: timedelta, action_set: ActionSetBase):
    max_action_id, input_timespans = action_set.input_timespans()

    # The input timespans grouped by the `margin` method
    input_islands = timeline.margin(offset, input_timespans)
    # The list out unprocssed inputs, determined by the `basic` method
    # we do not care about the max_timespans here, because the action_set
    # has not changed, so neither has the max_action_id
    unprocessed_timespans = action_set.basic()[1]

    result_timespans = list(get_islands(input_islands, unprocessed_timespans))
    # Sorting here. We don't really care about the order, but non-determinism
    # can be detrimental to a debuggers health.
    result_timespans.sort()

    return max_action_id, result_timespans
