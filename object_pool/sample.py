# coding: utf-8
from random import randrange
from time import sleep, perf_counter

from pool import *


class SlowConstructedObject(PooledObject):
    def __init__(self):
        # 模拟一个用时很长的对象构造
        sleep(1.0)

        super().__init__()


pool = ObjectPool(object_factory=SlowConstructedObject)

# 获得 3 个对象
begin_time = perf_counter()
obj_list = [pool.get_object() for _ in range(3)]
end_time = perf_counter()
print(f'time for new 3 objects: {end_time - begin_time:.3f}')  # 这里应该耗时 3 秒
# 获得这 3 个对象的 ID
obj_id_list = [id(obj) for obj in obj_list]

print(f'obj_id_list = {obj_id_list}')

# 随机还回 1 个对象
pool.release(obj_list[randrange(0, len(obj_id_list))])
# 再索取 1 个对象
begin_time = perf_counter()
obj_again = pool.get_object()
end_time = perf_counter()
print(f'time for fetch 1 object again: {end_time - begin_time:.3f}')  # 这里应该接近不耗时

print(f'id(obj_again) = {id(obj_again)}')

# 这个对象是一开始取出的 3 个对象之一
print(id(obj_again) in obj_id_list)

# 销毁所有对象
pool.reset()
