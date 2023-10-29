# coding: utf-8
import time

from loguru import logger

from wheel_timer import WheelTimer


def task():
    logger.info(f"Task executed at {time.time()}")


if __name__ == '__main__':
    wheel = WheelTimer(ticks_per_slot=1, num_slots=10)
    wheel.add_task(task, 2)
    wheel.add_task(task, 5)
    wheel.add_task(task, 8)

    try:
        while True:
            pass
    except KeyboardInterrupt:
        pass
