from typing import Tuple, Sequence, Callable
from datetime import datetime, timedelta

import pymongo
from pymongo.collection import Collection

from timeline import Timeline

Interval = Tuple[datetime, datetime]


def hourly(interval: Interval) -> Interval:
    start, stop = interval
    assert start <= stop

    start = start.replace(minute=0, second=0, microsecond=0)

    if stop.minute > 0 or stop.second > 0 or stop.microsecond > 0:
        stop = stop.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    return start, stop


class SensorBase:
    def __init__(self, analyzer_name: str, inputs: Sequence[str], action_log: Collection):
        self.name = analyzer_name
        self.log = action_log
        self.inputs = inputs

    def _find_last_run(self) -> int:
        docs = self.log.find({'action': 'analyze', 'name': self.name}).sort('_id', pymongo.DESCENDING).limit(1)
        try:
            return docs[0]['_id']
        except IndexError:
            return -1

    def _changes_since(self) -> pymongo.cursor.Cursor:
        action_id = self._find_last_run()
        return self.log.find({'_id': {'$gt': action_id}, 'outputs': {'$in': self.inputs}})

    def check(self) -> Sequence[Interval]:
        raise NotImplementedError()


class SensorNaive(SensorBase):
    def __init__(self, analyzer_name: str, inputs: Sequence[str], action_log: Collection):
        super().__init__(analyzer_name, inputs, action_log)

    def check(self) -> Sequence[Interval]:
        changes = self._changes_since()

        if changes.count() > 0:
            return [(datetime.min, datetime.max)]
        else:
            return []


class SensorAggregating(SensorBase):
    def __init__(self,
                 analyzer_name: str,
                 inputs: Sequence[str],
                 action_log: Collection,
                 extend_func: Callable[[Interval], Interval]):
        super().__init__(analyzer_name, inputs, action_log)
        self.extend_func = extend_func

    def check(self) -> Sequence[Interval]:
        changes = self._changes_since()

        print("changes:")
        for change in changes:
            print(change)

        changes.rewind()

        timespans = [self.extend_func(change['timespan']) for change in changes]

        tl = Timeline()
        for timespan in timespans:
            tl.add(timespan[0], timespan[1])

        return tl.intervals


class SensorDistance(SensorBase):
    # TODO
    def check(self) -> Sequence[Interval]:
        raise NotImplementedError()


