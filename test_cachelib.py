import itertools

import pytest

from cachelib import ARCache

def identity(x):
    return x

class TestARC:
    def test_arc(self):
        # Adapted from doctests in:
        # http://code.activestate.com/recipes/576532-adaptive-replacement-cache-in-python/
        arc = ARCache(maxsize=10, get_missing=identity)
        test_sequence = itertools.chain(
            range(20), range(11, 15), range(20), range(11, 40),
            (39, 38, 37, 36, 35, 34, 33, 32, 16, 17, 11, 41))
        for x in test_sequence:
            arc[x]
        assert list(arc.t1) == [41]
        assert list(arc.t2) == [11, 17, 16, 32, 33, 34, 35, 36, 37]
        assert list(arc.b1) == [31, 30]
        assert list(arc.b2) == [38, 39, 19, 18, 15, 14, 13, 12]
        assert int(arc.p) == 5
        assert arc.size == 10

    def test_delitem(self):
        arc = ARCache(maxsize=10, get_missing=identity)
        with pytest.raises(KeyError):
            del arc[42]
        for x in range(10):
            arc[x]
        for x in range(10):
            arc[x]
        arc[11]
        with pytest.raises(KeyError):
            del arc[0]
        for x in range(1, 10):
            del arc[x]
        del arc[11]
        assert arc.size == 0

