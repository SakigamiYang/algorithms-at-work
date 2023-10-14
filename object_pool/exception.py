# coding: utf-8
__all__ = ['PoolError', 'PoolFullError', 'PoolClosedError']


class PoolError(Exception):
    """General pool exception."""


class PoolFullError(PoolError):
    """Exception raised when create new object for a full-sized pool."""

    def __init__(self) -> None:
        super().__init__("The pool has reached its maximum size.")


class PoolClosedError(PoolError):
    """Exception raised when an operation is executed on a closed pool."""

    def __init__(self) -> None:
        super().__init__("Pool is closed")
