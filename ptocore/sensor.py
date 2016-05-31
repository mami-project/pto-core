from pymongo import MongoClient
from pymongo.collection import Collection

from .sensitivity import Sensitivity
from .analyzerstate import AnalyzerState


class Sensor:
    def __init__(self, analyzers_coll: Collection, action_log: Collection):
        self.analyzers_state = AnalyzerState(analyzers_coll)
        self.action_log = action_log

    def check(self):
        sensing = self.analyzers_state.sensing_analyzers()

        for analyzer in sensing:
            print(analyzer)
            blocked_types = self.analyzers_state.blocked_types()
            print("blocked_types:", blocked_types)
            if any(output_type in blocked_types for output_type in analyzer['output_types']):
                # TODO set 'stalled_reason' = "output blocked" in analyzers_coll
                continue

            unstable_types = self.analyzers_state.unstable_types()
            print("unstable_types:", blocked_types)
            if any(input_type in unstable_types for input_type in analyzer['input_types']):
                # TODO set 'stalled_reason' = "input unstable" in analyzers_coll
                continue

            stv = Sensitivity(self.action_log, analyzer['_id'], analyzer['input_formats'], analyzer['input_types'])

            if stv.any_changes():
                # okay let's do this. change state of analyzer to planned.
                self.analyzers_state.transition_to_planned(analyzer['_id'])

                # the input types and output types specified in the analyzer are now blocked

if __name__ == "__main__":
    mongo = MongoClient("mongodb://curator:ah8NSAdoITjT49M34VqZL3hEczCHjbcz@localhost/analysis")

    sens = Sensor(mongo.analysis.analyzers, mongo.analysis.action_log)

    sens.check()