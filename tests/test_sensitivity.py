import unittest
from datetime import datetime

from bson.objectid import ObjectId

from ptocore import sensitivity

upload_a = {
    '_id': 0,
    'action': 'upload',
    'output_formats': ['format0'],
    'timespans': [[datetime(2016, 6, 12, 4, 0), datetime(2016, 6, 12, 8, 0)]],
    'upload_ids': [ObjectId('A76670ee31e34a281d600a31')]
}

analyze_0 = {
    '_id': 1,
    'action': 'analyze',
    'analyzer_id': 'analyzerX',
    'output_types': ['type0'],
    'timespans': [
        [datetime(2016, 6, 12, 4, 0), datetime(2016, 6, 12, 8, 0)]
    ],
    'upload_ids': [ObjectId('A76670ee31e34a281d600a31')],
    'max_action_id': 0
}

upload_b = {
    '_id': 2,
    'action': 'upload',
    'output_formats': ['format0'],
    'timespans': [[datetime(2016, 6, 12, 6, 0), datetime(2016, 6, 12, 10, 0)]],
    'upload_ids': [ObjectId('B76670ee31e34a281d600a31')]
}

analyze_1 = {
    '_id': 3,
    'action': 'analyze',
    'analyzer_id': 'analyzerX',
    'output_types': ['type0'],
    'timespans': [
        [datetime(2016, 6, 12, 6, 0), datetime(2016, 6, 12, 10, 0)]
    ],
    'upload_ids': [ObjectId('B76670ee31e34a281d600a31')],
    'max_action_id': 2
}


class TestSensitivity(unittest.TestCase):
    def test_empty(self):
        """
        situation: empty observatory
        expected: do nothing
        """
        action_set = sensitivity.ActionSetTest(input_formats=['format0'], input_types=[],
                                               input_actions=[], output_actions=[])

        max_action_id, timespans = action_set.basic()

        self.assertEqual(max_action_id, -1)
        self.assertSequenceEqual(timespans, [])

    def test_single_upload(self):
        """
        situation: a single upload matching the analyzer.
        expected: analyze a
        """
        input_actions = [upload_a]

        action_set = sensitivity.ActionSetTest(input_formats=['format0'], input_types=[],
                                               input_actions=input_actions, output_actions=[])

        max_action_id, timespans = action_set.basic()

        self.assertEqual(max_action_id, 0)
        self.assertSequenceEqual(timespans, [(datetime(2016, 6, 12, 4, 0), datetime(2016, 6, 12, 8, 0))])

    def test_two_uploaded_none_analyzed(self):
        """
        situation: two uploads, none have been analyzed
        expected: analyze a and b
        """
        input_actions = [upload_a, upload_b]

        action_set = sensitivity.ActionSetTest(input_formats=['format0'], input_types=[],
                                               input_actions=input_actions, output_actions=[])

        max_action_id, timespans = action_set.basic()

        self.assertEqual(max_action_id, 2)
        self.assertSequenceEqual(timespans, [(datetime(2016, 6, 12, 4, 0), datetime(2016, 6, 12, 10, 0))])
