from itertools import chain

def merge(int1, int2):
    a, b = int1
    A, B = int2

    assert(a <= b)
    assert(A <= B)

    if A <= a <= b <= B:
        return (A, B)
    elif a <= A <= b <= B:
        return (a, B)
    elif A <= a <= B <= b:
        return (A, b)
    elif a <= A <= B <= b:
        return (a, b)
    elif b <= A:
        return None
    elif B <= a:
        return None


def subtract(int1, int2):
    a, b = int1
    A, B = int2

    assert(a <= b)
    assert(A <= B)

    if A <= a <= b <= B:
        return []
    elif a <= A <= b <= B:
        return [(a, A)]
    elif A <= a <= B <= b:
        return [(B, b)]
    elif a <= A <= B <= b:
        return [(a, A), (B, b)]
    elif b <= A:
        return [(a, b)]
    elif B <= a:
        return [(a, b)]


class Timeline:
    def __init__(self, intervals = None):
        if intervals is not None:
            self.intervals = intervals.copy()
        else:
            self.intervals = []

    def add_interval(self, a, b):
        assert(a <= b)

        candidate = (a, b)
        while True:
            for interval in self.intervals:
                merged = merge(candidate, interval)
                if merged is not None:
                    self.intervals.remove(interval)
                    candidate = merged
                    break
            else:
                self.intervals.append(candidate)
                break

    def remove_interval(self, a, b):
        assert(a <= b)

        candidate = (a, b)
        self.intervals = list(chain.from_iterable(subtract(interval, candidate) for interval in self.intervals))

    def is_empty(self):
        return len(self.intervals) == 0

    def __sub__(self, tl):
        ret = Timeline(self.intervals)
        for a, b in tl.intervals:
            ret.remove_interval(a, b)

        return ret

    def __add__(self, tl):
        ret = Timeline(self.intervals)
        for a, b in tl.intervals:
            ret.add_interval(a, b)

        return ret