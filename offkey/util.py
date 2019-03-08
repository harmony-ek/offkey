"""Utilities for offkey."""

import asyncio
from collections.abc import Sequence, Mapping


class Ticker:
    """
    Yield periodic tick times.

    The first tick is due at *start_time*.  Subsequent ticks are due at
    multiples of the given *period*.

    :param start_time: the start time; defaults to the current time.
    :param period: the period at which to yield ticks.
    """

    def __init__(self, *poargs, start_time=None, period, **kwargs):
        """Initialize this instance."""
        super().__init__(*poargs, **kwargs)
        if start_time is None:
            start_time = asyncio.get_running_loop().time()
        self.__start_time = start_time
        self.__period = period
        self.__ticks = 0
        self.__canceled = False

    @property
    def start_time(self):
        """Return the start time."""
        return self.__start_time

    @property
    def period(self):
        """Return the tick period."""
        return self.__period

    @property
    def ticks(self):
        """Return the number of ticks served so far."""
        return self.__ticks

    @property
    def canceled(self):
        """Return whether the ticker has been canceled."""
        return self.__canceled

    @property
    def last_tick(self):
        """Return the last served tick timestamp.

        Return `None` if none was served yet.
        """
        if self.__ticks == 0:
            return None
        return self.__start_time + (self.__ticks - 1) * self.__period

    @property
    def next_tick(self):
        """Return the next tick timestamp to be served; `None` if canceled."""
        if self.__canceled:
            return None
        return self.__start_time + self.__ticks * self.__period

    def cancel(self):
        """Cancel the ticker; make it generate no more ticks."""
        self.__canceled = True

    def __aiter__(self):
        """Return an async iterator."""
        return self

    async def __anext__(self):
        """Return the next element."""
        if self.__canceled:
            raise StopAsyncIteration
        get_time = asyncio.get_running_loop().time
        now = get_time()
        while now < self.next_tick:
            await asyncio.sleep(self.next_tick - now)
            now = get_time()
        self.__ticks += 1
        return self.last_tick


def flatten(o: object):
    """Flatten the given object."""
    yield from _flatten((), o)


def _flatten(pfx, o):
    if isinstance(o, Mapping):
        for k, v in o.items():
            yield from _flatten(pfx + (k,), v)
    elif isinstance(o, Sequence):
        for i, v in enumerate(o):
            yield from _flatten(pfx + (i,), v)
    else:
        yield pfx, o
