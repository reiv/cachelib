"""
cachelib
--------

"Cache rules everything around me"
    -- Method Man

Pure Python implementations of various cache algorithms.

License: MIT
"""

import collections
import threading

from collections import deque
from collections import Counter
from collections import OrderedDict

# Linked list indexes
LINK_PREV = 0
LINK_NEXT = 1
LINK_KEY = 2
LINK_VALUE = 3

# MQ-specific
LINK_ACCESS_COUNT = 4
LINK_LAST_ACCESS_TIME = 5
LINK_EXPIRE_TIME = 6

# ARC-specific
LINK_LIST_TYPE = 4
T1, T2, B1, B2 = 0, 1, 2, 3

def make_circular_queue():
    root = []
    root[:] = [root, root]
    return root


def _ll_move(link, target=None):
    prev, next = link[LINK_PREV], link[LINK_NEXT]
    if prev is not None:
        prev[LINK_NEXT] = next
    if next is not None:
        next[LINK_PREV] = prev
    if target is not None and target is not link:
        prev, next = target[LINK_PREV], target[LINK_NEXT]
        target[LINK_NEXT] = next[LINK_PREV] = link
        link[LINK_PREV] = target
        link[LINK_NEXT] = next

def _ll_iter_keys(root):
    link = root
    while True:
        link = link[LINK_PREV]
        if link is root:
            break
        yield link[LINK_KEY]

class Cache(collections.Mapping):
    def __init__(self, maxsize, get_missing=None, on_evict=None):
        self.maxsize = maxsize
        # Function which returns items missing from the cache.
        if get_missing:
            self.get_missing = get_missing
        # Callback for item eviction.
        self.on_evict = on_evict

        self.hits = self.misses = 0

        # Make linked list updates atomic.
        self._lock = threading.RLock()

    def get_missing(self, key):
        raise KeyError(key)

    def __contains__(self, key):
        return self._cache.__contains__(key)

    def __delitem__(self, key):
        raise NotImplementedError

    def __iter__(self):
        return iter(self._cache)

    def __len__(self):
        return self._cache.__len__()

    def discard(self, key):
        try:
            self.__delitem__(key)
        except KeyError:
            pass

    def flush(self):
        raise NotImplementedError

    def _hit(self):
        self.hits += 1

    def _miss(self):
        self.misses += 1

    @classmethod
    def cache(cls, *args, **kwargs):
        """
        Function decorator.
        """
        typed = kwargs.pop('typed', False)
        from functools import wraps, _make_key

        def decorator(f):
            kwargs['get_missing'] = f
            instance = cls(*args, **kwargs)

            @wraps(f)
            def wrapped(*args, **kwargs):
                key = _make_key(args, kwargs, typed)
                return instance[key]

            wrapped.__cache__ = instance

            return wrapped

        return decorator


class LRUCache(Cache):
    """
    Implementation of a cache with a last-recently-used replacement policy.

    LRU is susceptible to cache pollution caused by iterating over a
    sufficiently large number of unique items (scan vulnerability).
    """
    def __init__(self, *args, **kwargs):

        super(LRUCache, self).__init__(*args, **kwargs)

        # Mapping of keys to links.
        self._cache = {}

        self.size = 0

        # Cache consists of a doubly-linked list implementing a circular
        # queue with the following layout:
        # <- (prev) (next) ->
        # ItemN (MRU) <-> root (empty) <-> Item0 (LRU), Item1 <-> ...
        # The order of items in the queue is the eviction order.
        self._root = make_circular_queue()

    def __getitem__(self, key):
        root = self._root
        with self._lock:
            try:
                link = self._cache[key]
            except KeyError:
                # Cache miss.
                assert self._miss() or True

                value = self.get_missing(key)

                if self.size < self.maxsize:
                    # Cache can hold the new item. Create a new link and put it
                    # in the MRU (end of queue).
                    mru = root[LINK_PREV]
                    link = [mru, root, key, value]
                    self._cache[key] = mru[LINK_NEXT] = root[LINK_PREV] = link
                    self.size += 1
                else:
                    # Replace the current root with link.
                    root[LINK_KEY] = key
                    root[LINK_VALUE] = value
                    link = self._cache[key] = root

                    # Evict LRU.
                    lru = root[LINK_NEXT]
                    del self._cache[lru[LINK_KEY]]

                    # Clear LRU and make it the new root.
                    lru[LINK_KEY] = lru[LINK_VALUE] = None
                    self._root = lru

            else:
                # Cache hit.
                assert self._hit() or True

                prev, next, _, value = link

                # Remove the link from its current position.
                prev[LINK_NEXT] = next
                next[LINK_PREV] = prev

                # Move the link to the MRU (end of queue).
                mru = root[LINK_PREV]
                root[LINK_PREV] = mru[LINK_NEXT] = link
                link[LINK_PREV] = mru
                link[LINK_NEXT] = root

        return value

    def __delitem__(self, key):
        with self._lock:
            link = self._cache.pop(key)

            # Remove the link from its current position.
            prev = link[LINK_PREV]
            next = link[LINK_NEXT]
            link[:] = []

            prev[LINK_NEXT] = next
            next[LINK_PREV] = prev

            self.size -= 1


class MQCache(Cache):
    """
    Implementation of a cache using the Multi-Queue algorithm.

    Zhou, Yuanyuan, James Philbin, and Kai Li. "The Multi-Queue Replacement
    Algorithm for Second Level Buffer Caches." USENIX Annual Technical
    Conference, General Track. 2001.
    """

    def __init__(self, *args, m=2, q_out_factor=4, **kwargs):
        """
        By default, `MQCache` uses two queues, making it similar in principle
        to 2Q.

        The number of queues is determined by the adjustable parameter `m`.

        :param m: Number of LRU queues. At least 2 and usually fewer than 10.
        :param q_out_size: Number of items to keep in the eviction history.
        """

        super(MQCache, self).__init__(*args, **kwargs)

        # m = 1 doesn't give any benefits (it does exactly the same as LRU,
        # but with unnecessary overhead). m < 1 obviously doesn't make sense
        # at all.
        if m < 2:
            raise ValueError('m must be at least 2')

        self.m = m
        self.size = 0

        # Fast lookup. (Key -> Link)
        self._cache = {}

        # Eviction history.
        self._q_out = OrderedDict()
        self._q_out_size = int(self.maxsize * q_out_factor)

        # Temporal distance statistics.
        # Note: this only contains distances greater than maxsize.
        #       (i.e. of previously evicted items)
        self.temporal_distances = Counter()

        # LRU queue stack.
        self._queues = [make_circular_queue() for _ in range(m)]

        self.life_time = self.peak_temporal_distance()
        self.current_time = 0

    def __getitem__(self, key):
        q_out = self._q_out
        current_time = self.current_time

        with self._lock:
            try:
                link = self._cache[key]
            except KeyError:
                # Cache miss.

                if self.size < self.maxsize:
                    link = [None] * 7
                    self.size += 1
                else:
                    link = self._evict()
                    prev, next = link[LINK_PREV], link[LINK_NEXT]

                    # Remove the link from its current location.
                    prev[LINK_NEXT] = next
                    next[LINK_PREV] = prev

                link[LINK_VALUE] = value = self.get_missing(key)
                link[LINK_KEY] = key

                try:
                    access_count, last_access_time = q_out.pop(key)
                except KeyError:
                    access_count = 0
                else:
                    distance = current_time - last_access_time
                    self.temporal_distances[distance] += 1

            else:
                # Cache hit.
                prev, next, _, value, access_count, _, _ = link

                # Remove the link from its current location.
                prev[LINK_NEXT] = next
                next[LINK_PREV] = prev

            access_count += 1

            queue_num = self.queue_num(access_count)
            root = self._queues[queue_num]
            old_tail = root[LINK_PREV]

            # Insert link at tail of queue (MRU).
            old_tail[LINK_NEXT] = root[LINK_PREV] = link
            link[LINK_PREV] = old_tail
            link[LINK_NEXT] = root

            link[LINK_ACCESS_COUNT] = access_count
            link[LINK_LAST_ACCESS_TIME] = current_time
            link[LINK_EXPIRE_TIME] = current_time + self.life_time

            # Store fast reference.
            self._cache[key] = link

            self._adjust()

        return value

    def __delitem__(self, key):
        self._q_out.pop(key, None)
        LRUCache.__delitem__(self, key)


    def _evict(self):
        # Gotcha: it is the responsibility of the caller to remove or
        #         replace (potentially circular) references in the
        #         victim link's LINK_PREV and LINK_NEXT slots.

        # Get the first non-empty queue.
        q_out = self._q_out
        for k, root in enumerate(self._queues):
            # Get victim from head (LRU).
            link = root[LINK_NEXT]
            if link is not root:
                key = link[LINK_KEY]
                access_count = link[LINK_ACCESS_COUNT]
                last_access_time = link[LINK_LAST_ACCESS_TIME]
                prev, next = link[LINK_PREV], link[LINK_NEXT]

                # Remove the link.
                prev[LINK_NEXT] = next[LINK_PREV]
                next[LINK_PREV] = prev[LINK_NEXT]

                # Remove victim reference from internal dict.
                del self._cache[key]

                # Pop eviction history if it's full.
                if len(q_out) >= self._q_out_size:
                    q_out.popitem(last=False) # FIFO

                # Remember key, access count and last access time.
                q_out[key] = (access_count, last_access_time)

                # Invoke callback.
                on_evict = self.on_evict
                if on_evict is not None:
                    on_evict(key, link[LINK_VALUE])

                return link

        raise KeyError('cache is empty')


    def _adjust(self):
        # Intuition: This causes the head of each queue (except the first) to
        # 'age', possibly moving them to the tail end of a lower queue in the
        # stack. Without this, items with a high access count would linger in
        # the cache even after a drop in access frequency (a type of cache
        # pollution).

        self.current_time += 1

        for k in range(1, self.m):
            root = self._queues[k]
            link = root[LINK_NEXT]

            if link is root:
                # Queue is empty.
                continue

            if link[LINK_EXPIRE_TIME] < self.current_time:
                # Demote item to tail of previous queue in stack.
                root = self._queues[k-1]
                old_tail = root[LINK_PREV]
                old_tail[LINK_NEXT] = root[LINK_PREV] = link
                link[LINK_PREV] = old_tail
                link[LINK_NEXT] = root

                link[LINK_EXPIRE_TIME] = (
                    self.current_time + self.life_time)


    def queue_num(self, access_count):
        import math
        return min(int(math.log(access_count, 2)), self.m - 1)

    def peak_temporal_distance(self):
        """
        "[...] the peak temporal distance is defined as the temporal distance
        that is greater than the number of cache blocks and that has the most
        number of accesses." (Zhou 2001)

        In theory, MQ performance improves as life_time approaches the peak
        temporal distance.
        """
        try:
            return self.temporal_distances.most_common(1)[0][0]
        except IndexError:
            return self.maxsize + 1


class ARCache(Cache):
    """
    Implementation of a cache using the ARC algorithm.

    Megiddo, Nimrod, and Dharmendra S. Modha. "ARC: A Self-Tuning, Low
    Overhead Replacement Cache." FAST. Vol. 3. 2003.
    """
    def __init__(self, *args, **kwargs):
        super(ARCache, self).__init__(*args, **kwargs)

        self._cache = {}

        self._p = 0
        self._t1 = t1 = make_circular_queue()
        self._t2 = t2 = make_circular_queue()
        self._b1 = b1 = make_circular_queue()
        self._b2 = b2 = make_circular_queue()
        self._t1_len = self._t2_len = 0
        self._b1_len = self._b2_len = 0

    def __getitem__(self, key):
        t1_len = self._t1_len
        t2_len = self._t2_len
        b1_len = self._b1_len
        b2_len = self._b2_len
        maxsize = self.maxsize

        with self._lock:
            try:
                link = self._cache[key]

            # Case IV: Complete cache miss.
            except KeyError:
                assert self._miss() or True

                value = None
                self._cache[key] = link = [None, None, key, None, T1]

                # Case A: T1 U B1 has `maxsize` items.
                if t1_len + b1_len >= maxsize:
                    if b1_len:
                        # Delete LRU in B1.
                        victim = self._b1[LINK_NEXT]
                        assert victim is not self._b1
                        _ll_move(victim)
                        self._b1_len -= 1
                        self._replace(T1)
                    else:
                        # Delete LRU in T1.
                        victim = self._t1[LINK_NEXT]
                        assert victim is not self._t1
                        _ll_move(victim)
                        self._t1_len -= 1

                # Case B: T1 U B1 has fewer than `maxsize` items.
                else:
                    total = t1_len + t2_len + b1_len + b2_len
                    victim = None
                    if total >= maxsize:
                        if total == 2 * maxsize:
                            # Delete LRU in B2.
                            victim = self._b2[LINK_NEXT]
                            assert victim is not self._b2
                            _ll_move(victim)
                            self._b2_len -= 1
                        self._replace(T1)

                if victim is not None:
                    victim_key = victim[LINK_KEY]
                    del self._cache[victim_key]
                    if self.on_evict is not None:
                        self.on_evict(victim_key)
                    victim[:] = ()

                # Move new link to MRU of T1.
                _ll_move(link, self._t1[LINK_PREV])
                self._t1_len += 1

            # Case I, II or III: Cache hit or ghost cache hit.
            else:
                value, list_type = link[LINK_VALUE], link[LINK_LIST_TYPE]

                # Case II: Key is in B1.
                if list_type is B1:
                    assert self._miss() or True
                    # Update p.
                    d1 = 1 if b1_len >= b2_len else b2_len / b1_len
                    self._p = min(self._p + d1, maxsize)
                    self._replace(list_type)
                    self._b1_len -= 1
                    self._t2_len += 1

                # Case III: Key is in B2.
                elif list_type is B2:
                    assert self._miss() or True
                    # Update p.
                    d2 = 1 if b2_len >= b1_len else b1_len / b2_len
                    self._p = max(self._p - d2, 0)
                    self._replace(list_type)
                    self._b2_len -= 1
                    self._t2_len += 1

                # Case I: Key is in T1 or T2.
                else:
                    if list_type is T1:
                        self._t1_len -= 1
                        self._t2_len += 1
                    assert self._hit() or True

                # Move to MRU of T2.
                _ll_move(link, self._t2[LINK_PREV])
                link[LINK_LIST_TYPE] = T2

            if value is None:
                link[LINK_VALUE] = value = self.get_missing(key)

        return value

    def _replace(self, list_type):
        t1_len = self._t1_len
        p = self._p

        if t1_len and (t1_len > p or
            (list_type is B2 and t1_len == p)):
            # Delete the LRU in T1, move to MRU of B1.
            link, target = self._t1[LINK_NEXT], self._b1[LINK_PREV]
            link[LINK_LIST_TYPE] = B1
            self._t1_len -= 1
            self._b1_len += 1
        else:
            # Delete the LRU in T2, move to MRU of B2.
            link, target = self._t2[LINK_NEXT], self._b2[LINK_PREV]
            link[LINK_LIST_TYPE] = B2
            self._t2_len -= 1
            self._b2_len += 1

        with self._lock:
            # Remove reference to value.
            link[LINK_VALUE] = None
            _ll_move(link, target)

        # This counts as an eviction, although the key remains in B1/B2.
        if self.on_evict is not None:
            self.on_evict(link[LINK_KEY])

    def __delitem__(self, key):
        link = self._cache[key]
        list_type = link[LINK_LIST_TYPE]
        if list_type is T1:
            self._t1_len -= 1
        elif list_type is T2:
            self._t2_len -= 1
        else:
            # XXX: How should items in B1/B2 be handled, seeing as their
            # values aren't actually cached? Attempting to delete them sounds
            # like an error to me.
            raise KeyError(key)
        _ll_move(link)

    def __contains__(self, key):
        try:
            link = self._cache[key]
        except KeyError:
            return False
        if link[LINK_LIST_TYPE] not in (T1, T2):
            # XXX: See above.
            return False
        return True

    @property
    def size(self):
        return self._t1_len + self._t2_len

    @property
    def t1(self): return _ll_iter_keys(self._t1)

    @property
    def t2(self): return _ll_iter_keys(self._t2)

    @property
    def b1(self): return _ll_iter_keys(self._b1)

    @property
    def b2(self): return _ll_iter_keys(self._b2)

    @property
    def p(self): return self._p
