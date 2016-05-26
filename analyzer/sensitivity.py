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

def changes_since(action_log: Collection, analyzer_name: str, inputs: Sequence[str]) -> Tuple[int, pymongo.cursor.Cursor]:
    last_run_id = find_last_run(action_log, analyzer_name)
    return last_run_id, action_log.find({'_id': {'$gt': last_run_id}, 'outputs': {'$in': inputs}})

def basic(action_log: Collection, analyzer_name: str, inputs: Sequence[str]) -> Tuple[int, Sequence[Interval]]:
    """
    Note: it is important that the cursor is iterated only once, because otherwise it could happen for example that
          max_action_id was computed from a different set than tl.intervals.
    """
    last_run_id, changes = changes_since(action_log, analyzer_name, inputs)

    tl = Timeline()
    max_action_id = last_run_id
    for change in changes:
        start, end = change['timespan']
        tl.add(start, end)

        if max_action_id < change['_id']:
            max_action_id = change['_id']

    return max_action_id, tl.intervals

def naive(action_log: Collection, analyzer_name: str, inputs: Sequence[str]) -> Sequence[Interval]:
    last_run_id, changes = changes_since(action_log, analyzer_name, inputs)

    max_doc = list(changes.sort('_id', pymongo.DESCENDING).limit(1))
    max_action_id = max_doc[0]['_id'] if len(max_doc) > 0 else last_run_id

    if last_run_id != max_action_id:
        return max_action_id, [(datetime.min, datetime.max)]
    else:
        return max_action_id, []

def extend_hourly(interval: Interval) -> Interval:
    start, stop = interval
    assert start <= stop

    start = start.replace(minute=0, second=0, microsecond=0)

    if stop.minute > 0 or stop.second > 0 or stop.microsecond > 0:
        stop = stop.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    return start, stop

def aggregating(
        extend_func: Callable[[Interval], Interval],
        action_log: Collection,
        analyzer_name: str,
        inputs: Sequence[str]) -> Sequence[Interval]:

    last_run_id, changes = changes_since(action_log, analyzer_name, inputs)

    tl = Timeline()
    max_action_id = last_run_id
    for change in changes:
        timespan = extend_func(change['timespan'])
        tl.add(timespan[0], timespan[1])

        if max_action_id < change['_id']:
            max_action_id = change['_id']

    return max_action_id, tl.intervals

