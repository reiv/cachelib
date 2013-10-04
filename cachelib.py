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
LINK_ACCESS_COUNT = 4
LINK_LAST_ACCESS_TIME = 5
LINK_EXPIRE_TIME = 6

def make_circular_queue(slots):
    root = []
    root[:] = [root, root] + [None] * slots
    return root

class Cache(collections.Mapping):
    def __init__(self, maxsize, get_missing=None, on_evict=None):
        self.maxsize = maxsize
        # Function which returns items missing from the cache.
        if get_missing:
            self.get_missing = get_missing
        # Callback for item eviction.
        self.on_evict = on_evict

        self.size = 0
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

        # Cache consists of a doubly-linked list implementing a circular
        # queue with the following layout:
        # <- (prev) (next) ->
        # ItemN (MRU) <-> root (empty) <-> Item0 (LRU), Item1 <-> ...
        # The order of items in the queue is the eviction order.
        self._root = make_circular_queue(slots=3)

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

                prev, next, _, value, _ = link

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
        self._queues = [make_circular_queue(slots=4) for _ in range(m)]

        self.life_time = self.maxsize
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
                    link = self.evict()
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
                prev, next, _, value, access_count, _ = link

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

        self.adjust()

        return value


    def evict(self):
        # Gotcha: it is the responsibility of the caller to remove or
        #         replace (potentially circular) references in the
        #         victim link's LINK_PREV and LINK_NEXT slots.

        with self._lock:
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


    def adjust(self):
        # Intuition: This causes the head of each queue (except the first) to
        # 'age', possibly moving them to the tail end of a lower queue in the
        # stack. Without this, items with a high access count would linger in
        # the cache even after a drop in access frequency (a type of cache
        # pollution).

        with self._lock:

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
