# encoding: utf-8
#
# Copyright (C) 2010 Alec Thomas <alec@swapoff.org>
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.
#
# Author: Alec Thomas <alec@swapoff.org>


"""Distributed-computing primitives based on Redis."""


__author__ = 'Alec Thomas <alec@swapoff.org>'


from Queue import Empty
from redis import Redis
import json
import time


class DistributedError(Exception):
    """Base exception for distributed primitives."""


class LockTimeout(DistributedError):
    """Raised when lock acquisition times out."""


class Serializable(object):
    """Subclasses are serializable by the distributed-system API."""

    def serialize(self):
        """Serialize this object."""
        return dumps(self)


class _DistributedBase(object):
    """Base class for distributed objects."""

    def __init__(self, key=None, redis=None, namespace=None):
        if key is None:
            key = str(Counter(':keys', redis=redis).increment())
        self.key = key
        namespace = namespace or self.__class__.__name__.lower()
        self._key = 'distributed:%s:%s' % (namespace, self.key)
        self._redis = redis or _connection


class Queue(_DistributedBase, Serializable):
    """A distributed Queue."""

    def qsize(self):
        return self._redis.llen(self._key)

    def empty(self):
        return not self.qsize()

    def put(self, item):
        self._redis.rpush(self._key, dumps(item))

    def get(self, block=True, timeout=None):
        if block:
            value = self._redis.blpop(self._key, timeout=timeout or 0)
            if value is not None:
                value = value[1]
        else:
            value = self._redis.lpop(self._key)
        if value is None:
            raise Empty()
        return loads(value)


class Counter(_DistributedBase, Serializable):
    """An ever-incrementing counter, guaranteed to be unique."""

    def increment(self):
        """Increment the counter and return its current value."""
        return self._redis.incr('distributed:counter:' + self.key)


class Event(_DistributedBase, Serializable):
    """A distributed event.

    Protocol:
        Each event uses a key distributed:event:<key> as the current state of
        the event. If the key does not exist, the event is not set. If it
        exists, it is set.

        When an event is set(), the key is set to a value and a pubsub event
        sent under the channel distributed:event:<key>.
    """

    def wait(self):
        if self.is_set():
            return True
        # TODO: There is a race-condition here. A pubsub message may be sent
        # after checking is_set() and before starting the pubsub listen.
        pubsub = self._redis.pubsub()
        pubsub.subscribe(self._key)
        try:
            if self.is_set():
                return True
            for m in pubsub.listen():
                if m['type'] == 'message':
                    return True
        finally:
            pubsub.unsubscribe(self._key)

    def is_set(self):
        return self._redis.exists(self._key)

    def set(self):
        self._redis.set(self._key, '1')
        self._redis.publish(self._key, '1')

    def clear(self):
        self._redis.delete(self._key)


class Lock(_DistributedBase, Serializable):
    """A distributed lock."""

    # Code from retools:
    # https://github.com/bbangert/retools/blob/master/retools/lock.py

    def __init__(self, key=None, expires=60, timeout=10, redis=None):
        super(Lock, self).__init__(key, redis)
        self._expires = expires
        self._timeout = timeout
        self._start_time = time.time()

    def acquire(self, blocking=True):
        timeout = self._timeout
        while timeout >= 0:
            expires = time.time() + self._expires + 1

            if self._redis.setnx(self._key, expires):
                # We gained the lock; enter critical section
                self._start_time = time.time()
                return True

            current_value = self._redis.get(self._key)

            # We found an expired lock and nobody raced us to replacing it
            if current_value and float(current_value) < time.time() and \
                    self._redis.getset(self._key, expires) == current_value:
                self._start_time = time.time()
                return True

            if not blocking:
                return False

            timeout -= 1
            time.sleep(1)
        return False

    def release(self):
        # Only delete the key if we completed within the lock expiration,
        # otherwise, another lock might've been established
        if time.time() - self._start_time < self._expires:
            self._redis.delete(self._key)

    def __enter__(self):
        if not self.acquire():
            raise LockTimeout('Timed out waiting to acquire distributed lock')

    def __exit__(self, unused_exc_type, unused_exc_value, unused_traceback):
        return self.release()


class _CustomEncoder(json.JSONEncoder):
    """A custom JSON encoder for distributed primitives."""
    def default(self, instance):
        if isinstance(instance, Serializable):
            if hasattr(instance, '__serialize__'):
                state = instance.__serialize__()
            else:
                state = instance.__dict__.copy()
                for key in state.keys():
                    if key.startswith('_'):
                        del state[key]
            type = instance.__class__.__name__
            return {'__distributed_type__': type, 'state': state}
        return json.JSONEncoder.default(self, instance)


def dumps(instance):
    """Serialize POD types or distributed types for distributed use.

    The format for distributed types is deliberately very simple, serializing
    to a well-defined JSON format, to allow cross-language implementations.

    The format is a dictionary with two keys:
        __distributed_type__: Distributed type name eg. Lock
        state: State necessary to recreate instance.
    """
    return json.dumps(instance, cls=_CustomEncoder)


def _custom_decoder(pod):
    if isinstance(pod, dict) and '__distributed_type__' in pod:
        cls = globals()[pod['__distributed_type__']]
        if hasattr(cls, '__deserialize__'):
            instance = cls.__new__(cls)
            instance.__deserialize__(pod['state'])
        else:
            kwargs = dict((str(k), v) for k, v in pod['state'].iteritems())
            instance = cls(**kwargs)
        return instance
    return pod


def loads(state):
    """Deserialize data for use in a distributed system."""
    return json.loads(state, object_hook=_custom_decoder)


# Global Redis connections
_connection = None


def init(host='localhost', port=6379, db=0):
    """Initialize global distributed state.

    Must be called before any distributed primitives can be used.

    :param host: Redis host.
    :param port: Redis port.
    :param db: Redis DB slot.
    """
    global _connection
    _connection = Redis(host, port, db)
