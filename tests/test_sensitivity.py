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

    def test_new_old(self):
        input_actions = [
            {'action': 'analyze', 'upload_ids': [ObjectId('5773850731e34a206bde8ab6')], '_id': 82, 'timespans': [[datetime(2016, 6, 28, 0, 0), datetime(2016, 6, 29, 0, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('5773850731e34a206bde8ab6')], '_id': 75, 'timespans': [[datetime(2016, 6, 28, 0, 0), datetime(2016, 6, 29, 0, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('5773850731e34a206bde8ab6')], '_id': 73, 'timespans': [[datetime(2016, 6, 28, 0, 0), datetime(2016, 6, 29, 0, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('5773850731e34a206bde8ab6')], '_id': 71, 'timespans': [[datetime(2016, 6, 28, 0, 0), datetime(2016, 6, 29, 0, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('5773850731e34a206bde8ab6')], '_id': 69, 'timespans': [[datetime(2016, 6, 28, 0, 0), datetime(2016, 6, 29, 0, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('576670ee31e34a281d600a31')], '_id': 67, 'timespans': [[datetime(2016, 6, 12, 0, 0), datetime(2016, 6, 13, 0, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('5773850731e34a206bde8ab6')], '_id': 66, 'timespans': [[datetime(2016, 6, 28, 0, 0), datetime(2016, 6, 29, 0, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('5774a52d31e34a206bde8abc')], '_id': 65, 'timespans': [[datetime(2016, 6, 29, 0, 0), datetime(2016, 6, 30, 0, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('5774a27631e34a206bde8ab9')], '_id': 64, 'timespans': [[datetime(2016, 6, 29, 0, 0), datetime(2016, 6, 30, 0, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('5767a53731e34a6c3925a72b')], '_id': 63, 'timespans': [[datetime(1970, 1, 14, 11, 58, 0, 64000), datetime(1970, 1, 14, 17, 13, 13, 64000)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('578f74ba31e34a206bde8ac0')], '_id': 62, 'timespans': [[datetime(2016, 7, 18, 0, 0), datetime(2016, 7, 19, 1, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('578f74dc31e34a206bde8ac3')], '_id': 61, 'timespans': [[datetime(2016, 7, 18, 0, 0), datetime(2016, 7, 19, 23, 0)]]},
            {'action': 'analyze', 'upload_ids': [ObjectId('578f752331e34a206bde8ac6')], '_id': 60, 'timespans': [[datetime(2016, 7, 18, 0, 0), datetime(2016, 7, 19, 23, 0)]]}
        ]

        output_actions = [
            {'max_action_id': 75, 'upload_ids': None, '_id': 81, 'timespans': [[datetime(2016, 6, 12, 0, 0), datetime(2016, 6, 13, 0, 0)]]},
            {'max_action_id': 75, 'upload_ids': None, '_id': 80, 'timespans': [[datetime(2016, 6, 28, 0, 0), datetime(2016, 6, 30, 0, 0)]]},
            {'max_action_id': 75, 'upload_ids': None, '_id': 79, 'timespans': [[datetime(1970, 1, 14, 11, 58, 0, 64000), datetime(1970, 1, 14, 17, 13, 13, 64000)]]},
            {'max_action_id': 75, 'upload_ids': None, '_id': 78, 'timespans': [[datetime(2016, 7, 18, 0, 0), datetime(2016, 7, 19, 23, 0)]]}
        ]

        action_set = sensitivity.ActionSetTest(input_formats=[], input_types=['type0'],
                                               input_actions=input_actions, output_actions=output_actions)

        max_action_id, timespans = action_set.basic()

        self.assertEqual(max_action_id, 82)
        self.assertSequenceEqual(timespans, [(datetime(2016, 6, 28, 0, 0), datetime(2016, 6, 29, 0, 0))])
