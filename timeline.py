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
        return None
    elif B <= a:
        return None


class Timeline:
    def __init__(self):
        self.intervals = []

    def add(self, a, b):
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
