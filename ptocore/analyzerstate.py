from datetime import datetime
from typing import Tuple, Sequence
from itertools import chain

from pymongo.collection import Collection

Interval = Tuple[datetime, datetime]

class AnalyzerStateError(Exception):
    pass


class UnknownDomain(AnalyzerStateError):
    pass


class TransitionNotSupportedError(AnalyzerStateError):
    pass


class TransitionFailed(AnalyzerStateError):
    pass


transition_domains = {
    'admin': {
        'error':        {'disabled'},
        'disabled':     {'error', 'sensing'},
    },
    'sensor': {
        'sensing':      {'error', 'disabled', 'planned'}
    },
    'supervisor': {
        'planned':      {'error', 'executing'},
        'executing':    {'error', 'executed'}
    },
    'validator': {
        'executed':     {'error', 'validating'},
        'validating':   {'error', 'sensing'}
    }
}

all_states = set(chain.from_iterable(transition_domains.values()))
passive_states = set(transition_domains['admin']) | set(transition_domains['sensor'])
running_states = all_states - passive_states


class AnalyzerState:
    def __init__(self, domain, analyzers_coll: Collection):
        if domain not in transition_domains:
            raise UnknownDomain()

        self.domain = domain
        self.analyzers_coll = analyzers_coll

    def is_allowed(self, prev_state, next_state):
        return next_state in transition_domains[self.domain][prev_state]

    @staticmethod
    def state_to_domain(state):
        for domain, states in transition_domains.items():
            if state in states:
                return domain
        return None

    def __getitem__(self, analyzer_id):
        return self.analyzers_coll.find_one({'_id': analyzer_id})

    def is_in_my_domain(self, analyzer: dict):
        return self.state_to_domain(analyzer['state']) != self.domain

    def running_analyzers(self):
        return self.analyzers_coll.find({'state': {'$in': running_states}})

    def sensing_analyzers(self):
        return self.analyzers_coll.find({'state': 'sensing'})

    def planned_analyzers(self):
        return self.analyzers_coll.find({'state': 'planned'})

    def executed_analyzers(self):
        return self.analyzers_coll.find({'state': 'executed'})

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

    def create_analyzer(self, analyzer_id, input_formats, input_types, output_types, command_line, working_dir):
        """
        :raises pymongo.DuplicateKeyError if analyzer already exists
        """
        doc = {
            '_id': analyzer_id,
            'state': 'disabled',
            'input_formats': input_formats,
            'input_types': input_types,
            'output_types': output_types,
            'command_line': command_line,
            'working_dir': working_dir,
            'wish': None,
            'error': None
        }

        self.analyzers_coll.insert_one(doc)

    def request_wish(self, analyzer_id, wish):
        assert(wish in ['cancel', 'disable'])

        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id},
            {'$set': {'wish': wish}}
        )

        return doc is not None

    def transition(self, analyzer_id, prev_state, next_state, args: dict=None):
        # formulate queries
        find_query = {'_id': analyzer_id, 'state': prev_state}
        update_query = {'$set': {'state': next_state, 'error': None}}

        # add additional arguments
        if isinstance(args, dict):
            update_query['$set'].update(args)

        # check if transition is allowed
        if self.is_allowed(find_query['state'], update_query['$set']['state']):
            raise TransitionNotSupportedError()

        doc = self.analyzers_coll.find_one_and_update(find_query, update_query)

        if doc is None:
            raise TransitionFailed("analyzer '{}' not known or in other state that '{}'".format(analyzer_id, prev_state))

    def transition_to_error(self, analyzer_id, reason: str):
        # check if analyzer is in our domain
        doc = self[analyzer_id]
        if doc is None:
            raise TransitionFailed("analyzer '{}' not known".format(analyzer_id))

        if self.is_in_my_domain(doc):
            raise TransitionFailed('cannot transition to error, '
                                   'because state \'{}\' is not in our domain'.format(doc['state']))

        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id},
            {'$set': {'state': 'error', 'error': (self.domain, reason)}})

        if doc is None:
            raise TransitionFailed("analyzer '{}' not known or in other state that '{}'".format(analyzer_id, doc['state']))

    def check_wish(self, analyzer, granting_wish):
        # check if analyzer is in our domain and if we want to fulfil the wish if any
        if self.is_in_my_domain(analyzer) and granting_wish == analyzer['wish']:
            if granting_wish == 'disable':
                self.transition(analyzer['_id'], analyzer['state'], 'disabled')
            elif granting_wish == 'cancel':
                self.transition_to_error(analyzer['_id'], 'cancelled')
            else:
                raise AnalyzerStateError("granting_wish '{}' is not supported".format(granting_wish))

            return True
        else:
            return False

"""
    def transition_sensing_to_planned(self, analyzer_id):
        assert('planned' in transitions_allowed['sensing'])

        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': 'sensing'},
            {'$set': {'state': 'planned'}})

        return doc is not None

    def transition_planned_to_executing(self, analyzer_id, action_id):
        assert('executing' in transitions_allowed['planned'])

        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': 'planned'},
            {'$set': {'state': 'executing', 'action_id': action_id}})

        return doc is not None

    def transition_executing_to_executed(self, analyzer_id, temporary_coll_name: str, max_action_id: int, timespans: Sequence[Interval]):
        assert('executed' in transitions_allowed['executing'])

        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': 'executing'},
            {'$set': {'state': 'executed',
                      'execution_result': {'max_action_id': max_action_id, 'timespans': timespans, 'temporary_coll': temporary_coll_name}}})

        return doc is not None

    def transition_executed_to_validating(self, analyzer_id):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': 'executed'},
            {'$set': {'state': 'validating'}})

        return doc is not None

    def transition_to_validated(self, analyzer_id):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': {'$in': ['disabled', 'validating']}},
            {'$set': {'state': 'validated'},
             '$unset': {'error_reason': None, 'validation_params': None}})

        return doc is not None

    def transition_to_sensing(self, analyzer_id):
        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id, 'state': {'$in': ['disabled', 'validated']}},
            {'$set': {'state': 'validated'},
             '$unset': {'error_reason': None, 'validation_params': None}})

        return doc is not None

"""

