# encoding: utf-8
#
# Copyright (C) 2010 Alec Thomas <alec@swapoff.org>
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.
#
# Author: Alec Thomas <alec@swapoff.org>


"""A generic resource pool."""


__author__ = 'Alec Thomas <alec@swapoff.org>'


from Queue import Queue


class Resource(object):
    """Wrapper object around an allocated resource from ResourcePool.

    The underlying resource should generally by only accessed through this
    classes context-manager interface.
    """

    __slots__ = ('_pool', 'resource')

    def __init__(self, pool, resource):
        self._pool = pool
        self.resource = resource

    def release(self):
        """Release the underlying resource back into the pool."""
        self._pool.release(self.resource)

    def __enter__(self):
        return self.resource

    def __exit__(self, unused_type, unused_val, unused_tb):
        self.release()

    def __repr__(self):
        return 'Resource(%r)' % self.resource


class ResourcePool(object):
    """A generic resource pool.

        >>> class MyResource(object):
        ...     def __init__(self, name):
        ...         self.name = name
        >>> pool = ResourcePool([MyResource('one'), MyResource('two')])
        >>> with pool.acquire() as resource:
            ...     print resource.name
    """

    def __init__(self, resources):
        """Create a new resource pool populated with resources."""
        self._resources = Queue()
        for resource in resources:
            self._resources.put(resource)

    def acquire(self, timeout=None):
        """Acquire a resource.

        This should generally be only accessed through the context-manager
        interface:

            >>> with pool.acquire() as resource:
            ...     print resource.name

        :param timeout: If provided, time to wait for a resource before raising
                        Queue.Empty. If not provided, blocks indefinitely.

        :returns: Returns a Resource() wrapper object.
        :raises Empty: No resources are available before timeout.
        """
        if timeout is None:
            resource = self._resources.get()
        else:
            resource = self._resources.get(True, timeout)
        return Resource(self, resource)

    def release(self, resource):
        """Add a resource to the pool."""
        self._resources.put(resource)

    def empty(self):
        """Check if any resources are available.

        Note: This is a rough guide only. It does not guarantee that acquire()
              will succeed.
        """
        return self._resources.empty()
