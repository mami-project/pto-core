from time import sleep
import argparse
import logging

from pymongo.collection import Collection

from . import sensitivity
from .analyzerstate import AnalyzerState
from .coreconfig import CoreConfig
from . import repomanager


class Sensor:
    def __init__(self, analyzers_coll: Collection, action_log: Collection):
        self.analyzer_state = AnalyzerState('sensor', analyzers_coll)
        self.action_log = action_log

    def check(self):
        logger = logging.getLogger('sensor')

        logger.info("check for work")
        sensing = self.analyzer_state.sensing_analyzers()
        for analyzer in sensing:
            # check for wishes
            if self.analyzer_state.check_wish(analyzer, 'disable'):
                logger.info("disabled {} upon request".format(analyzer['_id']))
                continue

            if self.analyzer_state.check_wish(analyzer, 'cancel'):
                logger.info("cancelled {} upon request".format(analyzer['_id']))
                continue

            logger.debug("check situation for {}: input_types={}, input_formats={}"
                         .format(analyzer['_id'],analyzer['input_formats'], analyzer['input_types']))
            # check types
            blocked_types = self.analyzer_state.blocked_types()
            logger.debug("blocked_types: {}".format(str(blocked_types)))
            if any(output_type in blocked_types for output_type in analyzer['output_types']):
                # TODO set 'stalled_reason' = "output blocked" in analyzers_coll
                continue

            unstable_types = self.analyzer_state.unstable_types()
            logger.debug("unstable_types: {}".format(str(unstable_types)))
            if any(input_type in unstable_types for input_type in analyzer['input_types']):
                # TODO set 'stalled_reason' = "input unstable" in analyzers_coll
                continue

            repo_path = analyzer['working_dir']
            git_url = repomanager.get_repository_url(repo_path)
            git_commit = repomanager.get_repository_commit(repo_path)

            action_set = sensitivity.ActionSetMongo(analyzer['_id'], git_url, git_commit, analyzer['input_formats'],
                                                    analyzer['input_types'], self.action_log)

            logger.debug('input_action_set: {}'.format(action_set.input_actions))
            logger.debug('output_action_set: {}'.format(action_set.output_actions))

            if action_set.has_unprocessed_data(analyzer['direct']):
                logger.info('order execution of {}'.format(analyzer['_id']))
                # okay let's do this. change state of analyzer to planned.
                self.analyzer_state.transition(analyzer['_id'], 'sensing', 'planned')

                # the input types and output types specified in the analyzer are now blocked


def main():
    desc = 'Monitor the observatory for changes and order execution of analyzer modules.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('CONFIG_FILES', type=argparse.FileType('rt'), nargs='*')
    args = parser.parse_args()

    cc = CoreConfig('sensor', args.CONFIG_FILES)

    logging.basicConfig(level=logging.DEBUG)

    sens = Sensor(cc.analyzers_coll, cc.action_log)
    while True:
        sens.check()
        sleep(4)

if __name__ == "__main__":
    main()
