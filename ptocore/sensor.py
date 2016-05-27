from pymongo import MongoClient
from pymongo.collection import Collection

from . import sensitivity
from .analyzerstate import AnalyzerState


class Sensor:
    def __init__(self, analyzers_coll: Collection, action_log: Collection):
        self.analyzers_state = AnalyzerState(analyzers_coll)
        self.action_log = action_log

    def check(self):
        """
        1. get all idle production analyzers [name, input types]
        2. signalled those which probably need to run by performing base sensitivity check
        3. filter
        """

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

            max_action_id, timespans = sensitivity.basic(self.action_log, analyzer['_id'], analyzer['input_types'], analyzer['input_formats'])
            print("params: ", max_action_id, timespans)
            if len(timespans) > 0:
                # okay let's do this. change state of analyzer to planned.
                self.analyzers_state.transition_to_planned(analyzer['_id'],
                                                           {'max_action_id': max_action_id, 'timespans': timespans})

                # next time we call blocked_types() and unstable_types(), the input and output types of
                # this analyzer are now also showing up there.

if __name__ == "__main__":
    mongo = MongoClient("mongodb://curator:ah8NSAdoITjT49M34VqZL3hEczCHjbcz@localhost/analysis")

    sens = Sensor(mongo.analysis.analyzers, mongo.analysis.action_log)

    sens.check()
