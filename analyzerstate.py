from pymongo.collection import Collection
from itertools import chain

running_states = ['planned', 'executing', 'validating']
passive_states = ['sensing', 'disabled', 'error']

allowed_transitions = [
    # sensor domain
    ('disabled', 'sensing'),
    ('disabled', 'error'),
    ('error', 'sensing'),
    ('error', 'disabled'),
    ('sensing', 'error'),
    ('sensing', 'disabled'),
    ('sensing', 'planned'),

    # supervisor domain
    ('planned', 'error'),
    ('planned', 'executing'),
    ('executing', 'error'),
    ('executing', 'executed'),

    # validator domain
    ('executed', 'error'),
    ('executed', 'validating'),
    ('validating', 'error'),
    ('validating', 'sensing')
]

class AnalyzerState:
    def __init__(self, analyzers_coll: Collection):
        self.analyzers_coll = analyzers_coll

    def running_analyzers(self):
        return self.analyzers_coll.find({'state': {'$in': running_states}})

    def sensing_analyzers(self):
        return self.analyzers_coll.find({'state': 'sensing'})

    def planned_analyzers(self):
        return self.analyzers_coll.find({'state': 'planned'})

    def blocked_types(self):
        """
        Determines a list of observation types that are in the input specification of at least one running analyzer.
        """
        return set(chain.from_iterable(doc['input_types'] for doc in self.running_analyzers()))

    def unstable_types(self):
        """
        Determines a list of observation types that are in the output specification of at least one running analyzer.
        """
        return set(chain.from_iterable(doc['output_types'] for doc in self.running_analyzers()))

    def transition_to_planned(self, analyzer_id, execution_params):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': 'sensing'},
            {'$set': {'state': 'planned', 'execution_params': execution_params}})

        return doc is not None

    def transition_to_executing(self, analyzer_id, action_id):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': 'planned'},
            {'$set': {'state': 'executing', 'action_id': action_id}})

        return doc is not None

    def transition_to_executed(self, analyzer_id):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': 'executing'},
            {'$set': {'state': 'executed'}})

        return doc is not None

    def transition_to_validating(self, analyzer_id):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': 'executed'},
            {'$set': {'state': 'validating'}})

        return doc is not None

    def transition_to_sensing(self, analyzer_id):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': {'$in': ['error', 'disabled', 'finalizing']}},
            {'$set': {'state': 'sensing'},
             '$unset': {'action_id': None, 'error_reason': None, 'execution_params': None, 'validation_params': None}})

        return doc is not None

    def transition_to_error(self, analyzer_id, reason):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id},
            {'$set': {'state': 'error', 'error_reason': reason}})

        return doc is not None
