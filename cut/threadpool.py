# encoding: utf-8
#
# Copyright (C) 2010 Alec Thomas <alec@swapoff.org>
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.
#
# Author: Alec Thomas <alec@swapoff.org>


"""A generic thread-pool."""


__author__ = 'Alec Thomas <alec@swapoff.org>'


import logging
import threading
import sys

from collections import namedtuple
from Queue import Queue, Empty


WorkRequest = namedtuple('WorkRequest', 'callable args kwargs result')


class WorkResult(object):
    """Result of a request to the thread pool."""

    def __init__(self):
        self._ready = threading.Event()

    def get(self, timeout=None):
        """Get worker result.

        :param timeout: Optional timeout in seconds to wait for a result.

        :raises Empty: Thrown if a timeout was specified, and a
                       result was not ready in time.
        :raises Exception: If an exception occurred in the worker thread, it
                           will be reraised.
        """
        if not self.wait(timeout):
            raise Empty()
        if self.exc_info:
            # Re-raise original exception.
            raise self.exc_info[1], None, self.exc_info[2]
        return self.value

    def wait(self, timeout=None):
        """wait for the worker thread to return a result.

        :returns: True if we have a result before the timeout, False otherwise.
        """
        self._ready.wait(timeout)
        return self._ready.is_set()

    def ready(self):
        """Has the worker for this result finished?"""
        return self._ready.is_set()

    def successful(self):
        """Was the task successfully completed without error?

        :returns: True if we have a result, and it is not an exception.
        """
        return self._ready.is_set() and not self.exc_info

    def _put(self, value, exc_info):
        self.value = value
        self.exc_info = exc_info
        self._ready.set()


class ThreadPool(object):
    """Run callables in a pool of threads.

    May be used as a context manager for transient pools:

        >>> with ThreadPool(10) as pool:
            ...     return pool.map(MyFunc, my_args)
    """

    def __init__(self, workers):
        self._requests = Queue()
        self._results = {}
        self._workers = [threading.Thread(target=self._worker)
                         for _ in range(workers)]
        for worker in self._workers:
            worker.setDaemon(True)
            worker.start()

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.shutdown()

    def map(self, callable, sequence, timeout=None):
        """Convenience function.

        Returns a sequence of return values from callable.

        If a timeout is reached on any individual call, or an exception is
        raised, the resulting value in the sequence will be None.
        """
        work_results = [self.apply(callable, element) for element in sequence]
        results = []
        for result in work_results:
            try:
                results.append(result.get(timeout=timeout))
            except:
                results.append(None)
        return results

    def map_async(self, callable, sequence, timeout=None):
        """Call callable with each element of sequence.

        :returns: A list of WorkResult objects.
        """
        return [self.apply(callable, element, timeout=timeout)
                for element in sequence]

    def apply(self, callable, *args, **kwargs):
        """Run a function in a thread.

        :returns: Result of function call.
        """
        return self.apply_async(callable, *args, **kwargs).get()

    def apply_async(self, callable, *args, **kwargs):
        """Run a function in a thread.

        :returns: WorkResult object.
        """
        result = WorkResult()
        request = WorkRequest(callable, args, kwargs, result)
        self._requests.put(request)
        return result

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        """Shut the pool down, waiting for all workers to complete."""
        for _ in self._workers:
            self._requests.put(None)
        while self._workers:
            worker = self._workers.pop()
            worker.join()

    def _worker(self):
        while True:
            request = self._requests.get()
            if request is None:
                break
            try:
                value = request.callable(*request.args, **request.kwargs)
                request.result._put(value, None)
            except:
                exc_info = sys.exc_info()
                # Exceptions are re-raised in receiver.
                logging.error('ThreadPool worker %r(%r, %r) failed.',
                              request.callable, request.args, request.kwargs,
                              exc_info=exc_info)
                request.result._put(None, exc_info)
