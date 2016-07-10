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

class WrongState(AnalyzerStateError):
    pass


transition_domains = {
    'admin': {
        'error':        {'disabled', 'sensing'},
        'disabled':     {'error', 'sensing', 'planned'},
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

all_states = list(chain.from_iterable(transition_domains.values()))
passive_states = list(set(transition_domains['admin']) | set(transition_domains['sensor']))
running_states = list(set(all_states) - set(passive_states))


class AnalyzerState:
    def __init__(self, domain, analyzers_coll: Collection):
        if domain not in transition_domains:
            raise UnknownDomain()

        self.domain = domain
        self.analyzers_coll = analyzers_coll

    def is_allowed(self, prev_state, next_state):
        try:
            return next_state in transition_domains[self.domain][prev_state]
        except KeyError as e:
            return False

    @staticmethod
    def state_to_domain(state):
        for domain, states in transition_domains.items():
            if state in states:
                return domain
        return None

    def __getitem__(self, analyzer_id):
        return self.analyzers_coll.find_one({'_id': analyzer_id})

    def in_my_domain(self, analyzer: dict):
        return self.state_to_domain(analyzer['state']) == self.domain

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

    def update_analyzer(self, analyzer_id, input_formats=None, input_types=None, output_types=None, command_line=None):
        if not self[analyzer_id]['state'] == 'disabled':
            raise WrongState("updating analyzer only allowed in disabled state.")

        query = {}
        if input_formats is not None:
            query['input_formats'] = input_formats
        if input_types is not None:
            query['input_types'] = input_types
        if output_types is not None:
            query['output_types'] = output_types
        if command_line is not None:
            query['command_line'] = command_line

        self.analyzers_coll.update_one({'_id': analyzer_id}, {'$set': query})

    def request_wish(self, analyzer_id, wish):
        if wish not in ['cancel', 'disable']:
            raise ValueError("unknown wish")

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
        if not self.is_allowed(find_query['state'], update_query['$set']['state']):
            raise TransitionNotSupportedError()

        doc = self.analyzers_coll.find_one_and_update(find_query, update_query)

        if doc is None:
            raise TransitionFailed("analyzer '{}' not known or in other state that '{}'".format(analyzer_id, prev_state))

    def transition_to_error(self, analyzer_id, reason: str):
        # check if analyzer is in our domain
        doc = self[analyzer_id]
        if doc is None:
            raise TransitionFailed("analyzer '{}' not known".format(analyzer_id))

        if self.in_my_domain(doc) is False:
            raise TransitionFailed('cannot transition to error, '
                                   'because state \'{}\' is not in our domain'.format(doc['state']))

        doc = self.analyzers_coll.find_one_and_update(
            {'_id': analyzer_id},
            {'$set': {'state': 'error', 'error': (self.domain, reason)}})

        if doc is None:
            raise TransitionFailed("analyzer '{}' not known or in other state that '{}'".format(analyzer_id, doc['state']))

    def check_wish(self, analyzer, granting_wish):
        # check if analyzer is in our domain and if we want to fulfil the wish if any
        if self.in_my_domain(analyzer) and granting_wish == analyzer['wish']:
            if granting_wish == 'disable':
                self.transition(analyzer['_id'], analyzer['state'], 'disabled')
            elif granting_wish == 'cancel':
                self.transition_to_error(analyzer['_id'], 'cancelled')
            else:
                raise AnalyzerStateError("granting_wish '{}' is not supported".format(granting_wish))

            self.analyzers_coll.find_one_and_update(
                {'_id': analyzer['_id']}, {'$set': {'wish': None}}
            )

            return True
        else:
            return False
