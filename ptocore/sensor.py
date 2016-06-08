from pymongo import MongoClient
from pymongo.collection import Collection

from .sensitivity import Sensitivity
from .analyzerstate import AnalyzerState

from time import sleep


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

            stv = Sensitivity(self.action_log, analyzer['_id'], analyzer['input_formats'], analyzer['input_types'])

            if stv.any_changes():
                # okay let's do this. change state of analyzer to planned.
                self.analyzer_state.transition(analyzer['_id'], 'sensing', 'planned')

                # the input types and output types specified in the analyzer are now blocked

    def run(self):
        # TODO consider using threads
        while True:
            self.check()
            sleep(4)


def main():
    mongo = MongoClient("mongodb://curator:ah8NSAdoITjT49M34VqZL3hEczCHjbcz@localhost/analysis")

    sens = Sensor(mongo.analysis.analyzers, mongo.analysis.action_log)

    sens.run()

if __name__ == "__main__":
    main()
