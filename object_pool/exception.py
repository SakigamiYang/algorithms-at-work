# coding: utf-8
__all__ = [
    'TooManyObjectsError',
    'ChildDeadlockedError',
]


class PoolError(Exception):
    """对象池异常"""


class TooManyObjectsError(PoolError):
    """对象池超过最大容量异常"""


class ChildDeadlockedError(PoolError):
    """在子进程被 fork 后死锁异常"""
