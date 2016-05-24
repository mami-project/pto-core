from typing import Tuple, Sequence, Callable
from datetime import datetime, timedelta

import pymongo
from pymongo.collection import Collection

from timeline import Timeline

Interval = Tuple[datetime, datetime]

def find_last_run(action_log: Collection, analyzer_name: str) -> int:
    docs = action_log.find({'action': 'analyze', 'name': analyzer_name}).sort('_id', pymongo.DESCENDING).limit(1)
    try:
        return docs[0]['_id']
    except IndexError:
        return -1

def changes_since(action_log: Collection, analyzer_name: str, inputs: Sequence[str]) -> pymongo.cursor.Cursor:
    action_id = find_last_run(action_log, analyzer_name)
    return action_log.find({'_id': {'$gt': action_id}, 'outputs': {'$in': inputs}})

def basic(action_log: Collection, analyzer_name: str, inputs: Sequence[str]) -> Sequence[Interval]:
    changes = changes_since(action_log, analyzer_name, inputs)
    timespans = [change['timespan'] for change in changes]

    tl = Timeline()
    for timespan in timespans:
        tl.add(timespan[0], timespan[1])

    return tl.intervals

def naive(action_log: Collection, analyzer_name: str, inputs: Sequence[str]) -> Sequence[Interval]:
    changes = changes_since(action_log, analyzer_name, inputs)

    if changes.count() > 0:
        return [(datetime.min, datetime.max)]
    else:
        return []

def extend_hourly(interval: Interval) -> Interval:
    start, stop = interval
    assert start <= stop

    start = start.replace(minute=0, second=0, microsecond=0)

    if stop.minute > 0 or stop.second > 0 or stop.microsecond > 0:
        stop = stop.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    return start, stop

def aggregating(
        analyzer_name: str,
        inputs: Sequence[str],
        action_log: Collection,
        extend_func: Callable[[Interval], Interval]):

    changes = changes_since(action_log, analyzer_name, inputs)

    print("changes:")
    for change in changes:
        print(change)

    changes.rewind()

    timespans = [extend_func(change['timespan']) for change in changes]

    tl = Timeline()
    for timespan in timespans:
        tl.add(timespan[0], timespan[1])

    return tl.intervals

