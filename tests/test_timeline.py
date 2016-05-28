import unittest

from ptocore import timeline


class TestTimeline(unittest.TestCase):
    def test_1(self):
        tl = timeline.Timeline()
        tl.add(2, 4)
        self.assertSequenceEqual(tl.intervals, [(2, 4)])

    def test_2(self):
        tl = timeline.Timeline()
        tl.add(2, 4)
        tl.add(0, 2)
        self.assertSequenceEqual(tl.intervals, [(0, 4)])

    def test_3(self):
        tl = timeline.Timeline()
        tl.add(0, 1)
        tl.add(2, 3)
        self.assertSequenceEqual(tl.intervals, [(0, 1), (2, 3)])

    def test_4(self):
        tl = timeline.Timeline()
        tl.add(2, 3)
        tl.add(0, 1)
        self.assertSequenceEqual(tl.intervals, [(2, 3), (0, 1)])

if __name__ == '__main__':
    unittest.main()
