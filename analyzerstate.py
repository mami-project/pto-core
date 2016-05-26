from pymongo.collection import Collection
from itertools import chain

running_states = ['planned', 'executing', 'finalizing']
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
    ('executed', 'finalizing'),
    ('finalizing', 'error'),
    ('finalizing', 'sensing')
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
            {'state': 'planned', 'execution_params': execution_params})

        return doc is not None

    def transition_to_executing(self, analyzer_id, action_id):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': 'planned'},
            {'state': 'executing', 'action_id': action_id})

        return doc is not None

    def transition_to_executed(self, analyzer_id):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': 'executing'},
            {'state': 'executed'})

        return doc is not None

    def transition_to_error(self, analyzer_id, reason):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id},
            {'state': 'error', 'error_reason': reason})

        return doc is not None

    def set_state(self, analyzer_id, next_state: str):
        """
        Sets new state of analyzer.
        :return: Return previous state if successful. In case the analyzer is not found or
                 transition is not allowed, return None.
        """
        allowed_from = [transition[0] for transition in allowed_transitions if transition[1] == next_state]
        ret = self.analyzers_coll.find_one_and_update({'_id': analyzer_id, 'state': {'$in': allowed_from}}, {'state': next_state})

        return ret['state'] if ret is not None else None
