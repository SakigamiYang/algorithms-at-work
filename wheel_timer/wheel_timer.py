# coding: utf-8
import threading
import time


class WheelTimer:
    def __init__(self, ticks_per_slot: int, num_slots: int):
        """
        构造方法
        :param ticks_per_slot: 时间轮中每个槽位代表的时间间隔。
                               以 Netty3 为例，在 Netty3 中，时间轮被用于检测 IO 超时。
                               由于 IO 用时的粒度比较粗，所以每 100ms 检测一次，
                               如果是并发要求更大的计算服务，这个值应酌情降低。
        :param num_slots: 时间轮中的槽位数量
        """
        self.ticks_per_slot = ticks_per_slot
        self.num_slots = num_slots
        self.slots = [[] for _ in range(num_slots)]
        self.current_tick = 0

        self._init_timer_thread()

    def _init_timer_thread(self):
        self.timer_thread = threading.Thread(target=self._run)
        self.timer_thread.daemon = True
        self.timer_thread.start()

    def _run(self):
        while True:
            tasks_to_run = self.slots[self.current_tick]
            for task in tasks_to_run:
                task()
            self.slots[self.current_tick] = []
            self.current_tick = (self.current_tick + 1) % self.num_slots
            time.sleep(self.ticks_per_slot)

    def add_task(self, task, delay):
        if delay < 0:
            delay = 0
        slot = (self.current_tick + (delay // self.ticks_per_slot)) % self.num_slots
        self.slots[slot].append(task)
