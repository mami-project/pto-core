from datetime import datetime, timedelta
from typing import Tuple, Sequence, Callable

import pymongo
from pymongo.collection import Collection

from ptocore.timeline import Timeline

Interval = Tuple[datetime, datetime]

def extend_hourly(interval: Interval) -> Interval:
    start, stop = interval
    assert start <= stop

    start = start.replace(minute=0, second=0, microsecond=0)

    if stop.minute > 0 or stop.second > 0 or stop.microsecond > 0:
        stop = stop.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    return start, stop

class Sensitivity:
    def __init__(self, action_log: Collection, analyzer_id: str,
                 input_formats: Sequence[str], input_types: Sequence[str]):
        self.action_log = action_log
        self.analyzer_id = analyzer_id
        self.input_formats = input_formats
        self.input_types = input_types

    def find_last_run(self) -> int:
        docs = self.action_log.find({'action': 'analyze', 'analyzer_id': self.analyzer_id})\
            .sort('_id', pymongo.DESCENDING).limit(1)
        try:
            return docs[0]['_id']
        except IndexError:
            return -1

    def changes_since(self) -> Tuple[int, pymongo.cursor.Cursor]:
        last_run_id = self.find_last_run()
        changes = self.action_log.find({'_id': {'$gt': last_run_id},
                                   '$or': [{'output_types': {'$in': self.input_types}},
                                           {'output_formats': {'$in': self.input_formats}}]
                                   })

        return last_run_id, changes

    def any_changes(self) -> bool:
        _, changes = self.changes_since()
        return changes.count() > 0

    def basic(self) -> Tuple[int, Sequence[Interval]]:
        """
        Note: it is important that the cursor is iterated only once, because otherwise it could happen for example that
              max_action_id was computed from a different set than tl.intervals.
        """
        last_run_id, changes = self.changes_since()

        tl = Timeline()
        max_action_id = last_run_id
        for change in changes:
            start, end = change['timespan']
            tl.add(start, end)

            if max_action_id < change['_id']:
                max_action_id = change['_id']

        return max_action_id, tl.intervals

    def naive(self) -> Sequence[Interval]:
        last_run_id, changes = self.changes_since()

        max_doc = list(changes.sort('_id', pymongo.DESCENDING).limit(1))

        max_action_id = max_doc[0]['_id'] if len(max_doc) > 0 else last_run_id

        if last_run_id != max_action_id:
            return max_action_id, [(datetime.min, datetime.max)]
        else:
            return max_action_id, []

    def aggregating(self, extend_func: Callable[[Interval], Interval]) -> Sequence[Interval]:
        last_run_id, changes = self.changes_since()

        tl = Timeline()
        max_action_id = last_run_id
        for change in changes:
            timespan = extend_func(change['timespan'])
            tl.add(timespan[0], timespan[1])

            if max_action_id < change['_id']:
                max_action_id = change['_id']

        return max_action_id, tl.intervals

