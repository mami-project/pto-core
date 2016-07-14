from datetime import datetime
from typing import Sequence, Tuple

from bson import CodecOptions
from collections import OrderedDict
from pymongo.collection import Collection

VALIDATION_COMPARE_FIELDS = {'conditions', 'time', 'path', 'value', 'sources', 'analyzer_id'}

VALIDATION_INPUT_FIELDS = VALIDATION_COMPARE_FIELDS | {'_id'}

VALIDATION_OUTPUT_FIELDS = VALIDATION_COMPARE_FIELDS | {'action_ids', 'valid'}

COMPARE_PROJECTION = {'_id': 0, 'conditions': 1, 'path': 1, 'analyzer_id': 1, 'sources': 1, 'value': 1}


class ValidationError(Exception):
    def __init__(self, obsid, reason: str, extra: str=''):
        self.obsid = obsid
        self.reason = reason
        self.extra = extra

    def __repr__(self):
        return "Validation Error {}: {} {}".format(self.obsid, self.reason, self.extra)


codec_opts = CodecOptions(document_class=OrderedDict)


def collection_ensure_order(coll: Collection):
    return coll.with_options(codec_options=codec_opts)


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

            # check that conditions are defined in output_types
            conditions = doc['conditions']
            check(all(condition in output_types for condition in conditions), obsid,
                  'condition(s) not declared in output_types', 'expected all of {} to be in {}'.format(conditions, output_types))

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
            # TODO
            #check(valuechecks.checks[condition](doc['value']), obsid, 'value')

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