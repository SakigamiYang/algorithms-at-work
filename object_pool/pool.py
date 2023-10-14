# coding: utf-8
import queue
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from queue import SimpleQueue
from threading import Event, Semaphore, Condition, BoundedSemaphore, Lock
from typing import Generic, TypeVar, Optional, Generator

from loguru import logger

from .exception import *

__all__ = ['Manager', 'Pool']

DEFAULT_SIZE = 10

_T = TypeVar("_T")


class Manager(Generic[_T], metaclass=ABCMeta):
    """A manager for the lifecycles of pool objects."""

    @abstractmethod
    def create(self) -> _T:
        """Create a new pool object."""

    @abstractmethod
    def recycle(self, __obj: _T) -> None:
        """Check liveness and reset released objects.

        If the object is no longer valid, this method should raise an exception
        to signal it and prevent its return to the pool.

        :arg __obj: The object to return.
        :raise user defined exception: When the object is no longer valid."""

    @abstractmethod
    def discard(self, __obj: _T) -> None:
        """Perform cleanup of discarded objects.

        This method is called for discarding both invalid objects that failed the
        recycling and live objects on pool closure. Liveness should not be assumed and
        this method should ideally not raise any exception unless there's a failure
        that will lead to a resource leak.

        :arg __obj: The object to be discarded."""


class PoolState(Generic[_T]):
    """An object that records the state of the pool,
    preventing thread resource concurrent problems"""
    __is_open: Event
    __count: Semaphore
    __lock: Condition
    __idle: SimpleQueue[_T]

    def __init__(self,
                 is_open: Event,
                 count: Semaphore,
                 lock: Condition,
                 idle: SimpleQueue[_T]):
        self.__is_open = is_open
        self.__count = count
        self.__lock = lock
        self.__idle = idle

    @property
    def is_open(self):
        return self.__is_open

    @property
    def count(self):
        return self.__count

    @property
    def lock(self):
        return self.__lock

    @property
    def idle(self):
        return self.__idle


class Pool(Generic[_T]):
    """Object pool."""

    __manager: Manager[_T]
    __max_size: int
    __state: PoolState[_T]

    def __init__(self, manager: Manager[_T], max_size: Optional[int] = None) -> None:
        """Initializer.

        :arg manager: The object manager to use.
        :arg max_size: Optional. The maximum number of concurrent objects available."""

        if max_size <= 0:
            raise ValueError("max_size must be at least 1")
        max_size = max_size or DEFAULT_SIZE
        self.__manager = manager
        self.__max_size = max_size
        self.__init_state()

    def __init_state(self) -> None:
        self.__state = PoolState(
            is_open=Event(),
            count=BoundedSemaphore(self.__max_size),
            lock=Condition(Lock()),
            idle=SimpleQueue(),
        )

    @property
    def is_open(self) -> bool:
        """Check if the pool is open.

        :return: Bool. Whether the pool is open."""

        return self.__state.is_open.is_set()

    def open(self) -> None:
        """Open the pool."""

        self.__state.is_open.set()

    def close(self) -> None:
        """Close the pool and discard its all objects."""

        state = self.__state

        if not state.is_open.is_set():
            return

        self.__init_state()
        state.is_open.clear()

        while True:
            try:
                pass
            except queue.Empty:
                break
            except Exception:
                logger.opt(exception=True).warning("Discard error, possible resource leak")

        with state.lock:
            state.lock.notify_all()

    @contextmanager
    def acquire(self) -> Generator[_T, None, None]:
        """Acquire an object from the pool.

        :return: A generator of pooled object."""

        state = self.__state

        while True:
            if not state.is_open.is_set():
                raise PoolClosedError()

            # Try getting object from the pool first
            try:
                obj = state.idle.get_nowait()
                logger.debug(f"Checked out object from pool: {obj}")
                break
            except queue.Empty:
                pass

            # If we can allocate more, create a new one
            if state.count.acquire(blocking=False):
                try:
                    obj = self.__manager.create()
                    logger.debug(f"Created new object: {obj}")
                    break
                except:
                    state.count.release()
                    raise PoolFullError()

            # Wait until an object is available or we can allocate more
            with state.lock:
                logger.debug("Waiting for free object or slot")
                state.lock.wait()

        try:
            yield obj
        finally:
            try:
                if not state.is_open.is_set():
                    raise PoolClosedError()

                self.__manager.recycle(obj)
                logger.debug(f"Object succeeded recycle: {obj}")

                if not state.is_open.is_set():
                    raise PoolClosedError()

                state.idle.put(obj)
                logger.debug(f"Object returned to pool: {obj}")
            except Exception:
                logger.opt(exception=True).debug(f"Recycle failed discarding: {obj}")
                try:
                    self.__manager.discard(obj)
                except Exception:
                    logger.opt(exception=True).warning("Discard error, possible resource leak")
                state.count.release()
            finally:
                with state.lock:
                    state.lock.notify()


_T_Pool = TypeVar("_T_Pool", bound=Pool)
