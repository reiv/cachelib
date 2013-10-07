cachelib
========

``cachelib`` is a Python library of caching algorithms.

Compatibility: Python 3.3+

Algorithms
----------

``cachelib`` contains implementations of the following cache algorithms:

- LRU (last recently used): ``LRUCache``
- MQ (multi-queue): ``MQCache``
- ARC (adaptive replacement cache): ``ARCache``

Usage
-----
Caches have the same mapping interface as ``dict`` ::

    >>> from cachelib import LRUCache
    >>> lru_cache = LRUCache(maxsize=10, get_missing=lambda x: x)
    >>> lru_cache['hello']
    'hello'

Items are evicted when the cache size exceeds ``maxsize``. On a cache miss,
the value is taken from the return value of ``get_missing``. You can also
specify a callback for evicted items: ::

    def callback(key):
        print('evicted {!r}'.format(key))

    LRUCache(10, lambda x: x, on_evict=callback)

Caches can be used as function decorators. ::

    @LRUCache.cache(maxsize=10)
    def foo(x):
        ...

This has the same effect as using ``functools.lru_cache``, but you can choose
from different algorithms.

License
-------
MIT.
