import unittest
from datetime import datetime, timedelta

from ptocore import timeline

class TestTimelineMargin(unittest.TestCase):
    def test_0(self):
        offset = timedelta(seconds=5)
        inp = []
        out = timeline.margin(offset, inp)

        self.assertSequenceEqual(out, [])

    def test_1(self):
        offset = 5
        inp = [(0, 5), (8, 10)]
        out = timeline.margin(offset, inp)

        self.assertSequenceEqual(out, [(0, 10)])

    def test_2(self):
        offset = 5
        inp = [(0, 5), (10, 15)]
        out = timeline.margin(offset, inp)

        self.assertSequenceEqual(out, [(0, 15)])

    def test_3(self):
        offset = 5
        inp = [(0, 5), (11, 15)]
        out = timeline.margin(offset, inp)

        self.assertSequenceEqual(out, [(11, 15), (0, 5)])

    def test_4(self):
        offset = 5
        inp = [(0, 5), (11, 15), (11, 15)]
        out = timeline.margin(offset, inp)

        self.assertSequenceEqual(out, [(11, 15), (0, 5)])

    def test_5(self):
        offset = 5
        inp = [(0, 5), (11, 15), (14, 20)]
        out = timeline.margin(offset, inp)

        self.assertSequenceEqual(out, [(11, 20), (0, 5)])

    def test_6(self):
        offset = timedelta(seconds=30)
        inp = [
            (datetime(2016, 6, 1,  0,  0,  0), datetime(2016, 6, 1,  0,  0,  45)),
            (datetime(2016, 6, 1,  0,  1, 15), datetime(2016, 6, 1,  0,  1,  30)),
            (datetime(2016, 6, 1,  0,  3,  0), datetime(2016, 6, 1,  0,  3,  45))
        ]
        out = timeline.margin(offset, inp)

        out_expect = [
            (datetime(2016, 6, 1,  0,  3,  0), datetime(2016, 6, 1,  0,  3,  45)),
            (datetime(2016, 6, 1,  0,  0,  0), datetime(2016, 6, 1,  0,  1,  30))
        ]

        self.assertSequenceEqual(out, out_expect)
