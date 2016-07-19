import unittest
from datetime import datetime

from bson.objectid import ObjectId

from ptocore import sensitivity

upload_a = {
    '_id': 0,
    'action': 'upload',
    'output_formats': ['format0'],
    'timespans': [[datetime(2016, 6, 12, 0, 0), datetime(2016, 6, 13, 0, 0)]],
    'upload_id': ObjectId('A76670ee31e34a281d600a31')
}

analyze_a = {
    '_id': 1,
    'action': 'analyze',
    'analyzer_id': 'analyzerX',
    'git_commit': 'commit',
    'git_url': 'url',
    'output_types': ['type0'],
    'timespans': [
        [datetime(2016, 6, 12, 0, 0), datetime(2016, 6, 13, 0, 0)]
    ],
    'upload_ids': [ObjectId('A76670ee31e34a281d600a31')]
}

upload_b = {
    '_id': 2,
    'action': 'upload',
    'output_formats': ['format0'],
    'timespans': [[datetime(2016, 6, 12, 0, 0), datetime(2016, 6, 13, 0, 0)]],
    'upload_id': ObjectId('B76670ee31e34a281d600a31')
}

analyze_b = {
    '_id': 3,
    'action': 'analyze',
    'analyzer_id': 'analyzerX',
    'git_commit': 'commit',
    'git_url': 'url',
    'output_types': ['type0'],
    'timespans': [
        [datetime(2016, 6, 12, 0, 0), datetime(2016, 6, 13, 0, 0)]
    ],
    'upload_ids': [ObjectId('B76670ee31e34a281d600a31')]
}

mark_invalid_a = {
    '_id': 4,
    'action': 'marked_invalid',
    'output_formats': ['format0'],
    'timespans': [[datetime(2016, 6, 12, 0, 0), datetime(2016, 6, 13, 0, 0)]],
    'upload_id': ObjectId('A76670ee31e34a281d600a31')
}

analyze_a_2 = {
    '_id': 5,
    'action': 'analyze',
    'analyzer_id': 'analyzerX',
    'git_commit': 'commit',
    'git_url': 'url',
    'output_types': ['type0'],
    'timespans': [
        [datetime(2016, 6, 12, 0, 0), datetime(2016, 6, 13, 0, 0)]
    ],
    'upload_ids': [ObjectId('A76670ee31e34a281d600a31')]
}

mark_valid_a = {
    '_id': 6,
    'action': 'marked_valid',
    'output_formats': ['format0'],
    'timespans': [[datetime(2016, 6, 12, 0, 0), datetime(2016, 6, 13, 0, 0)]],
    'upload_id': ObjectId('A76670ee31e34a281d600a31')
}

class TestSensitivityDirect(unittest.TestCase):
    def test_empty(self):
        """
        situation: empty observatory
        expected: do nothing
        """
        action_set = sensitivity.ActionSetTest(input_formats=['format0'], input_types=[],
                                               input_actions=[], output_actions=[])

        max_action_id, upload_ids = sensitivity.direct(action_set)

        self.assertEqual(max_action_id, -1)
        self.assertSequenceEqual(upload_ids, [])

    def test_single_upload(self):
        """
        situation: a single upload matching the analyzer.
        expected: analyze a
        """
        input_actions = [upload_a]

        action_set = sensitivity.ActionSetTest(input_formats=['format0'], input_types=[],
                                               input_actions=input_actions, output_actions=[])

        max_action_id, upload_ids = sensitivity.direct(action_set)

        self.assertEqual(max_action_id, 0)
        self.assertSequenceEqual(upload_ids, [ObjectId('A76670ee31e34a281d600a31')])

    def test_two_uploaded_none_analyzed(self):
        """
        situation: two uploads, none have been analyzed
        expected: analyze a and b
        """
        input_actions = [upload_a, upload_b]

        action_set = sensitivity.ActionSetTest(input_formats=['format0'], input_types=[],
                                               input_actions=input_actions, output_actions=[])

        max_action_id, upload_ids = sensitivity.direct(action_set)

        self.assertEqual(max_action_id, 2)
        self.assertSequenceEqual(upload_ids, [ObjectId('A76670ee31e34a281d600a31'), ObjectId('B76670ee31e34a281d600a31')])

    def test_two_uploaded_one_analyzed(self):
        """
        situation: two uploads, upload_a has been analyzed
        expected: analyze b
        """
        input_actions = [upload_a, upload_b]

        output_actions = [analyze_a]

        action_set = sensitivity.ActionSetTest(input_formats=['format0'], input_types=[],
                                               input_actions=input_actions, output_actions=output_actions)

        max_action_id, upload_ids = sensitivity.direct(action_set)

        self.assertEqual(max_action_id, 2)
        self.assertSequenceEqual(upload_ids, [ObjectId('B76670ee31e34a281d600a31')])

    def test_two_uploads_both_analyzed(self):
        """
        situation: two uploads, both have been analyzed
        expected: do nothing
        """
        input_actions = [upload_a, upload_b]

        output_actions = [analyze_a, analyze_b]

        action_set = sensitivity.ActionSetTest(input_formats=['format0'], input_types=[],
                                               input_actions=input_actions, output_actions=output_actions)

        max_action_id, upload_ids = sensitivity.direct(action_set)

        self.assertEqual(max_action_id, 3)
        self.assertSequenceEqual(upload_ids, [])

    def test_two_uploads_both_analyzed_one_invalid(self):
        """
        situation: two uploads, both have been analyzed, and upload_a has been marked invalid now
        expected: analyze a again
        """
        input_actions = [upload_a, upload_b, mark_invalid_a]

        output_actions = [analyze_a, analyze_b]

        action_set = sensitivity.ActionSetTest(input_formats=['format0'], input_types=[],
                                               input_actions=input_actions, output_actions=output_actions)

        max_action_id, upload_ids = sensitivity.direct(action_set)

        self.assertEqual(max_action_id, 4)
        self.assertSequenceEqual(upload_ids, [ObjectId('A76670ee31e34a281d600a31')])

    def test_two_uploads_both_analyzed_one_invalid_valid(self):
        """
        situation: two uploads, both have been analyzed, and upload_a has been marked
        invalid and now is marked valid again
        expected: analyze a again
        """
        input_actions = [upload_a, upload_b, mark_invalid_a, mark_valid_a]

        output_actions = [analyze_a, analyze_b]

        action_set = sensitivity.ActionSetTest(input_formats=['format0'], input_types=[],
                                               input_actions=input_actions, output_actions=output_actions)

        max_action_id, upload_ids = sensitivity.direct(action_set)

        self.assertEqual(max_action_id, 6)
        self.assertSequenceEqual(upload_ids, [ObjectId('A76670ee31e34a281d600a31')])
