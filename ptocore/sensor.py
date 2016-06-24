from time import sleep
import argparse

from pymongo.collection import Collection

from .sensitivity import Sensitivity, find_last_run
from .analyzerstate import AnalyzerState
from .coreconfig import CoreConfig
from . import repomanager


class Sensor:
    def __init__(self, analyzers_coll: Collection, action_log: Collection):
        self.analyzer_state = AnalyzerState('sensor', analyzers_coll)
        self.action_log = action_log

    def check(self):
        print("sensor: check for work")
        sensing = self.analyzer_state.sensing_analyzers()
        for analyzer in sensing:
            # check for wishes
            if self.analyzer_state.check_wish(analyzer, 'disable'):
                print("sensor: disabled {} upon request".format(analyzer['_id']))
                continue

            if self.analyzer_state.check_wish(analyzer, 'cancel'):
                print("sensor: cancelled {} upon request".format(analyzer['_id']))
                continue

            # check types
            blocked_types = self.analyzer_state.blocked_types()
            print("blocked_types:", blocked_types)
            if any(output_type in blocked_types for output_type in analyzer['output_types']):
                # TODO set 'stalled_reason' = "output blocked" in analyzers_coll
                continue

            unstable_types = self.analyzer_state.unstable_types()
            print("unstable_types:", blocked_types)
            if any(input_type in unstable_types for input_type in analyzer['input_types']):
                # TODO set 'stalled_reason' = "input unstable" in analyzers_coll
                continue

            repo_path = analyzer['working_dir']
            git_url = repomanager.get_repository_url(repo_path)
            git_commit = repomanager.get_repository_commit(repo_path)

            # check if code has changed and request rebuild if so.
            # if last_run is None, then the value of rebuild_all does not matter
            last_run = find_last_run(analyzer['_id'], self.action_log)
            rebuild_all = True
            if last_run is not None:
                if last_run['git_url'] == git_url and last_run['git_commit'] == git_commit:
                    rebuild_all = False

            # create sensitivity object, note that we don't know rebuild_all yet.
            stv = Sensitivity(self.action_log, analyzer['_id'], analyzer['input_formats'],
                              analyzer['input_types'], rebuild_all)

            # find out if code has changed since last run and decide if the analyzer
            # has to rebuild all observations

            if stv.any_changes():
                # okay let's do this. change state of analyzer to planned.
                self.analyzer_state.transition(analyzer['_id'], 'sensing', 'planned', {'rebuild_all': rebuild_all})

                # the input types and output types specified in the analyzer are now blocked


def main():
    desc = 'Monitor the observatory for changes and order execution of analyzer modules.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('CONFIG_FILES', type=argparse.FileType('rt'), nargs='*')
    args = parser.parse_args()

    cc = CoreConfig('sensor', args.CONFIG_FILES)

    sens = Sensor(cc.analyzers_coll, cc.action_log)
    while True:
        sens.check()
        sleep(4)

if __name__ == "__main__":
    main()
