from datetime import datetime
from typing import Sequence, Tuple
from time import sleep
import argparse

import re
from bson import ObjectId
import pymongo
from pymongo.errors import BulkWriteError
from pymongo.operations import UpdateOne, InsertOne

from .collutils import grouper_transpose
from .analyzerstate import AnalyzerState
from .mongoutils import AutoIncrementFactory
from .coreconfig import CoreConfig
from .commit import commit_direct, commit_normal

Interval = Tuple[datetime, datetime]

pattern_ip4 = re.compile(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")


class Validator:
    def __init__(self, core_config: CoreConfig):
        self.cc = core_config
        self.analyzer_state = AnalyzerState('validator', core_config.analyzers_coll)

        self.action_id_name = 'action_id.'+self.cc.environment
        self.valid_name = 'valid.'+self.cc.environment

        # the validator is the only component generating action_ids, therefore create_if_missing=True is not a problem.
        idfactory = AutoIncrementFactory(core_config.idfactory_coll)
        self._action_id_creator = idfactory.get_incrementor('action_id', create_if_missing=True)

    def validate_upload(self, upload_id: ObjectId, valid: bool):
        action_doc = self.cc.action_log.find_one({'upload_id': upload_id, 'action': 'upload'})
        if action_doc is None:
            print("upload doesn't exist in action_log")
            return

        upload_doc = self.cc.metadata_coll.find_one({'_id': upload_id, self.action_id_name: {'$exists': True}})
        if upload_doc is None:
            print("upload doesn't exist in upload database or upload has no field called '"+self.action_id_name+"'")
            return

        if not isinstance(upload_id, ObjectId):
            print("argument upload_id is not of type ObjectId.")

        if not isinstance(valid, bool):
            print("argument valid is not of type bool.")

        timespans = action_doc['timespans']
        output_formats = action_doc['output_formats']

        action = "marked_valid" if valid else "marked_invalid"

        action_id = self._action_id_creator()
        self.cc.metadata_coll.update_one({'_id': upload_id}, {'$set': {self.valid_name: valid}})
        self.cc.action_log.insert_one({ "_id" : action_id, "timespans" : timespans, "upload_id" : upload_id,
                              "action" : action, "output_formats" : output_formats })

    def check_for_requests(self):
        """
        Check for requests to change valid state of an upload.
        """
        while True:
            doc = self.cc.requests_coll.find_one_and_delete(
                {'receiver': 'validator'}, sort=[('_id', pymongo.ASCENDING)]
            )
            if doc is None:
                break

            if doc['action'] == 'validate_upload':
                print("fulfil request: set valid: {} for upload_id {}".format(doc['valid'], doc['upload_id']))
                self.validate_upload(doc['upload_id'], doc['valid'])

    def check_for_uploads(self):
        """
        Assign action_id to every completed upload
        """

        def set_action_id_ops() -> Sequence[Tuple[UpdateOne, InsertOne]]:
            find_query = {
                'complete': True,
                self.action_id_name: {'$exists': False},
                'meta.format': {'$exists': True},
                'meta.start_time': {'$exists': True},
                'meta.stop_time': {'$exists': True}
            }

            # apply filter from environment config
            if isinstance(self.cc.validator_upload_filter, dict):
                find_query.update(self.cc.validator_upload_filter)

            cursor = self.cc.metadata_coll.find(find_query).sort('timestamp')
            for upload in cursor:
                action_id = self._action_id_creator()
                print("assign action id {} to upload {}".format(action_id, upload['_id']))
                uploads_query = UpdateOne({'_id': upload['_id']},
                                          {'$set': {self.action_id_name: action_id,
                                                    self.valid_name: True}})

                timespans = [(upload['meta']['start_time'], upload['meta']['stop_time'])]

                action_log_query = InsertOne({
                    '_id': action_id,
                    'output_formats': [upload['meta']['format']],
                    'timespans': timespans,
                    'action': 'upload',
                    'upload_ids': [upload['_id']]
                })

                yield uploads_query, action_log_query

        try:
            for uploads_block, action_log_block in grouper_transpose(set_action_id_ops(), 1000):
                self.cc.metadata_coll.bulk_write(uploads_block)
                self.cc.action_log.bulk_write(action_log_block)
        except BulkWriteError as e:
            # most likely a configuration error
            print(e.details)
            raise


    def check_for_analyzers(self):
        executed = self.analyzer_state.executed_analyzers()
        for analyzer in executed:
            # check for wish
            if self.analyzer_state.check_wish(analyzer, 'cancel'):
                print("validator: cancelled {} upon request".format(analyzer['_id']))
                continue

            print("validating and committing {}".format(analyzer['_id']))

            self.analyzer_state.transition(analyzer['_id'], 'executed', 'validating')

            exe_res = analyzer['execution_result']
            temporary_coll = self.cc.temporary_db[exe_res['temporary_coll']]

            if exe_res['timespans'] is not None and exe_res['upload_ids'] is not None:
                self.analyzer_state.transition_to_error(analyzer['_id'],
                    'internal error: either timespans or upload_ids can have a '
                    'value but not both. I cannot decide if direct or normal analyzer')
                continue

            if exe_res['timespans'] is None and exe_res['upload_ids'] is None:
                self.analyzer_state.transition_to_error(analyzer['_id'],
                    'internal error: it\'s not allowed to have both timespans and upload_ids to be None. '
                    'I cannot decide if direct or normal analyzer')
                continue

            if exe_res['upload_ids'] is not None:
                print("using direct commit")
                valid_count, errors, action_id = commit_direct(
                    analyzer['_id'], analyzer['working_dir'], self._action_id_creator,
                    exe_res['upload_ids'], exe_res['max_action_id'], temporary_coll,
                    self.cc.observations_coll, analyzer['output_types'],
                    self.cc.action_log)
            else:
                print("using normal commit")
                valid_count, errors, action_id = commit_normal(
                    analyzer['_id'], analyzer['working_dir'], self._action_id_creator,
                    exe_res['timespans'], exe_res['max_action_id'], temporary_coll,
                    self.cc.observations_coll, analyzer['output_types'],
                    self.cc.action_log)

            if len(errors) > 0:
                print("analyzer {} with action id {} has at least {} valid records but {} have problems:".format(analyzer['_id'], action_id, valid_count, len(errors)))
                for idx, error in enumerate(errors):
                    print("{}: {}".format(idx, error))

                self.analyzer_state.transition_to_error(analyzer['_id'], 'error when executing validator:\n' + '\n'.join((str(error) for error in errors)))
            else:
                print("successfully commited analyzer {} run with action id {}. {} records inserted".format(analyzer['_id'], action_id, valid_count))
                self.analyzer_state.transition(analyzer['_id'], 'validating', 'sensing', {'action_id': action_id})


    def check_for_work(self):
        print("validator: check for work")
        self.check_for_analyzers()
        self.check_for_uploads()
        self.check_for_requests()


def main():
    desc = 'Monitor the observatory for changes and order execution of analyzer modules.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('CONFIG_FILES', type=argparse.FileType('rt'), nargs='*')
    args = parser.parse_args()

    cc = CoreConfig('validator', args.CONFIG_FILES)

    val = Validator(cc)

    while True:
        val.check_for_work()
        sleep(4)

if __name__ == "__main__":
    main()