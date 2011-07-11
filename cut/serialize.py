# encoding: utf-8
#
# Copyright (C) 2010 Alec Thomas <alec@swapoff.org>
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.
#
# Author: Alec Thomas <alec@swapoff.org>


"""A serialization layer.

The goal of this system is to serialize objects into a form that is
language-agnostic.
"""


__author__ = 'Alec Thomas <alec@swapoff.org>'


from abc import ABCMeta, abstractmethod
import collections
import json


class SerializableMeta(type):
    def __new__(mcs, name, bases, dict):
        cls = type.__new__(mcs, name, bases, dict)
        assert name.lower() not in _serializable, \
                'serialized types must have globally unique names'
        _serializable[name.lower()] = cls
        return cls


class Serializable(object):
    """A default serialization implementation.

    The serialized state is a copy of the objects __dict__ consisting of only
    public attributes.
    """
    __metaclass__ = SerializableMeta

    def __serialize__(self):
        state = self.__dict__.copy()
        for key in state.keys():
            if key.startswith('_'):
                del state[key]
        return state

    def __deserialize__(self, state):
        self.__dict__.update(state)


class Serializer(object):
    def __init__(self, impl):
        self.impl = impl

    def serialize(self, data):
        return self.impl.dumps(_to_pod(data))

    def deserialize(self, raw):
        return _from_pod(self.impl.loads(raw))


def _to_pod(instance):
    """Convert from a nested data structure with classes, to a POD."""
    if isinstance(instance, dict):
        if hasattr(instance, '__serialize__'):
            state = instance.__serialize__()
            type = instance.__class__.__name__.lower()
            instance = {'__type__': type, 'state': state}
        else:
            for k, v in instance.iteritems():
                instance[k] = _to_pod(v)
    elif isinstance(instance, collections.Iterable):
        return map(_to_pod, instance)
    return instance


def _from_pod(pod):
    """Recursively convert from POD to complex structures."""
    if isinstance(pod, dict):
        type = pod.get('__type__', None)
        if type is not None:
            cls = globals()[type]
            if hasattr(cls, '__deserialize__'):
                instance = cls.__new__(cls)
                instance.__deserialize__(pod['state'])
                return instance
            else:
                kwargs = dict((str(k), v) for k, v in pod['state'].iteritems())
                return cls(**kwargs)
        else:
            for key, value in pod.iteritems():
                pod[key] = _from_pod(value)
    elif isinstance(pod, collections.Iterable):
        return map(_from_pod, pod)
    return pod


_serializable = {}
