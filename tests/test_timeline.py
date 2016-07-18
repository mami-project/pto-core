import unittest

from ptocore import timeline


class TestTimeline(unittest.TestCase):
    def test_1(self):
        tl = timeline.Timeline()
        tl.add_interval(2, 4)
        self.assertSequenceEqual(tl.intervals, [(2, 4)])

    def test_2(self):
        tl = timeline.Timeline()
        tl.add_interval(2, 4)
        tl.add_interval(0, 2)
        self.assertSequenceEqual(tl.intervals, [(0, 4)])

    def test_3(self):
        tl = timeline.Timeline()
        tl.add_interval(0, 1)
        tl.add_interval(2, 3)
        self.assertSequenceEqual(tl.intervals, [(0, 1), (2, 3)])

    def test_4(self):
        tl = timeline.Timeline()
        tl.add_interval(2, 3)
        tl.add_interval(0, 1)
        self.assertSequenceEqual(tl.intervals, [(2, 3), (0, 1)])

    def test_tl_add_0(self):
        tl0 = timeline.Timeline()
        tl0.add_interval(0, 1)

        tl1 = timeline.Timeline()
        tl1.add_interval(2, 3)

        tl2 = tl0 + tl1
        self.assertSequenceEqual(tl2.intervals, [(0, 1), (2, 3)])

    def test_tl_add_1(self):
        tl0 = timeline.Timeline()
        tl0.add_interval(0, 1)

        tl1 = timeline.Timeline()
        tl1.add_interval(1, 2)

        tl2 = tl0 + tl1
        self.assertSequenceEqual(tl2.intervals, [(0, 2)])

    def test_tl_sub_0(self):
        tl0 = timeline.Timeline()
        tl0.add_interval(0, 4)

        tl1 = timeline.Timeline()
        tl1.add_interval(1, 2)

        tl2 = tl0 - tl1
        self.assertSequenceEqual(tl2.intervals, [(0, 1), (2, 4)])

    def test_tl_sub_1(self):
        tl0 = timeline.Timeline()
        tl0.add_interval(0, 4)

        tl1 = timeline.Timeline()
        tl1.add_interval(3, 5)

        tl2 = tl0 - tl1
        self.assertSequenceEqual(tl2.intervals, [(0, 3)])

    def test_tl_sub_2(self):
        tl0 = timeline.Timeline()
        tl0.add_interval(0, 4)

        tl1 = timeline.Timeline()
        tl1.add_interval(-1, 3)

        tl2 = tl0 - tl1
        self.assertSequenceEqual(tl2.intervals, [(3, 4)])

if __name__ == '__main__':
    unittest.main()
