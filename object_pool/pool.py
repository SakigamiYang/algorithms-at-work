# coding: utf-8
from os import getpid
from threading import Lock
from typing import Optional, TypeVar, Callable

from object_pool.exception import *

__all__ = ['PooledObject', 'ObjectPool']

DEFAULT_SIZE = 10


class PooledObject:
    """可池化对象
    一般这种对象的生产都要耗费巨大的资源，所以基本都支持一个 close() 方法。但并非必须的。"""

    def __init__(self):
        self._pid = getpid()  # 用来防止进程 fork 检测

    @property
    def pid(self):
        return self._pid

    def close(self):
        """释放对象资源"""
        pass


_T_PooledObject = TypeVar("_T_PooledObject", bound=PooledObject)


class ObjectPool:
    """对象池"""

    def __init__(self,
                 object_factory: Callable[..., _T_PooledObject],
                 max_size: Optional[int] = None):
        """初始化

        :arg object_factory: 对象生产工厂方法
        :arg max_size: 可选。对象池最大容量"""

        max_size = max_size or DEFAULT_SIZE
        if not isinstance(max_size, int) or max_size < 0:
            raise ValueError("'max_size' must be a positive integer")

        self._object_factory = object_factory
        self._max_size = max_size

        # 用来保护 _checkpid() 的锁，如果 pid 改变了，比如进程被 fork 了，该锁将会被获取。
        # 再此期间，子进程中的多个线程会抢锁，第一个抢到的线程会重置数据结构并锁定该池的对象。
        # 随后的线程在成功获取该锁并发现第一个线程完成工作后，释放该锁。
        self._fork_lock = Lock()
        self.reset()

    def reset(self) -> None:
        """重置线程池"""

        self._lock = Lock()
        self._created_objects = 0
        self._available_objects = []
        self._in_use_objects = set()

        # 注意这里必须是 reset() 函数的最后一句。
        # 因为如果先取得 pid ，则一个线程在执行 reset() 中时，
        # 其它线程执行了 _checkpid() ,就会获取到不一样的 pid ，
        # 而此时该线程的的 reset() 还没有执行完。
        self.pid = getpid()

    def _checkpid(self) -> None:
        """该方法用来在现代系统上保证 fork 安全。所有操作池状态的方法都会调用该方法，
        该方法通过比较在池对象中存储的进程 ID 和当前进程 ID 来确定进程是否被 fork 。
        如果不通，则子进程不能使用父进程的文件描述符（如套接字），此时子进程应该调用 reset() 方法来重置池对象。

        _checkpid() 将会由 self._fork_lock 来保证不会被子进程中的多个线程调用多次。

        但是这有一个非常小概率的失败场景：
          1. A 进程第一次调用 _checkpid() 并获取了锁 self._fork_lock。
          2. A 进程在持有锁的当中被其下某个线程 fork 了。
          3. fork 出的子进程 B 继承了父进程的池状态，该状态中伴有一个 _fork_lock ，而由于资源隔离，
             进程 B 永远不可能通知进程 A 释放该锁。

        为了避免这个死锁问题， _checkpid() 在获取锁时只会等待 5 秒。失败则判断为
        子进程死锁，并抛出 ChildDeadlockedError 异常。"""

        if self.pid != getpid():
            acquired = self._fork_lock.acquire(timeout=5.)
            if not acquired:
                raise ChildDeadlockedError
            # 调用 reset() ，如果其它线程没有这么干
            try:
                if self.pid != getpid():
                    self.reset()
            finally:
                self._fork_lock.release()

    def get_object(self) -> _T_PooledObject:
        """获得一个对象"""

        self._checkpid()
        with self._lock:
            try:
                obj = self._available_objects.pop()
            except IndexError:
                obj = self._make_object()
            self._in_use_objects.add(obj)
        return obj

    def _make_object(self) -> _T_PooledObject:
        if self._created_objects >= self._max_size:
            raise TooManyObjectsError
        self._created_objects += 1
        return self._object_factory()

    def release(self, obj: _T_PooledObject) -> None:
        """释放对象

        :arg obj: 要释放的对象"""

        self._checkpid()
        with self._lock:
            try:
                self._in_use_objects.remove(obj)
            except KeyError:
                pass

            if self._owns_object(obj):
                self._available_objects.append(obj)
            else:
                # 目标对象不由该对象池持有（比如进程被 fork 的情况），则直接扔掉，为对象池空出一个位置
                self._created_objects -= 1

    def _owns_object(self, obj: _T_PooledObject) -> bool:
        return obj.pid == self.pid
