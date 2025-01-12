import collections
import heapq
import itertools
import random
import time
import threading


def fake_io_read(future):
    def read():
        time.sleep(random.random())
        future.set_result(random.randint(1, 100))

    threading.Thread(target=read).start()


async def main_task():
    """一个主任务"""
    print("begin task", time.time())
    big_result = await big_step()
    print("end task", time.time())
    print("big_result is ", big_result)


async def big_step():
    """一个大任务"""
    print(" - begin big task", time.time())
    small_result = await small_step()
    print(" - end big task", time.time())
    return small_result + 1


async def small_step():
    global loop
    fut = Future()

    # 指派一个目标执行set_result
    fake_io_read(fut)  # 模拟一个IO操作  把Future和IO操作绑定到一起
    result = await fut
    return result


class Future:

    def __init__(self):
        global loop
        self._loop = loop
        self._result = None
        self._done = None
        self._callbacks = []

    def set_result(self, result):
        if self._done:
            raise RuntimeError("future already done.")
        self._result = result
        self._done = True

        for cb in self._callbacks:
            self._loop.call_soon(cb)

    def result(self):
        if self._done:
            return self._result
        else:
            raise RuntimeError("future is not done")

    def add_done_callback(self, callback):
        self._callbacks.append(callback)

    def __await__(self):
        yield self
        return self.result()


task_id_counter = itertools.count(1)


class Task(Future):
    """一个通用的任务处理"""

    def __init__(self, coro):
        super().__init__()
        # self._result = None
        # self._done = None
        self.coro = coro
        self._id = "Task-{}".format(next(task_id_counter))
        self._loop.call_soon(self.run)

    def run(self):
        print("--------Task {}-------".format(self._id))
        if not self._done:
            try:
                x = self.coro.send(None)
            except StopIteration as e:
                # self._result = e.value
                # self._done = True
                self.set_result(e.value)
            else:
                assert isinstance(x, Future)
                x.add_done_callback(self.run)  # 重新激活task
        print("---------------")


class EventLoop:
    """相当于一个任务调度器"""

    def __init__(self):
        self._ready = collections.deque()  # 一个双向任务队列，先入先出
        self._scheduled = []  # 定时任务，需要保持一个顺序，
        self._stopping = False  # 提供一个标志位，用来结束run_once中的循环

    def call_soon(self, callback, *args):
        """把任务加入任务队列"""
        self._ready.append((callback, args))

    def call_later(self, delay, callback, *args):
        """利用堆来存储定时任务"""
        t = time.time() + delay
        heapq.heappush(
            self._scheduled, (t, callback, args)
        )  ## 让时间最小的排在堆顶的位置

    def stop(self):
        self._stopping = True

    def run_forever(self):
        while True:
            self.run_once()  # 保证循环至少执行一轮
            if self._stopping:
                break

    def run_once(self):
        # 处理定时任务
        now = time.time()
        if self._scheduled:
            if self._scheduled[0][0] < now:
                _, cb, args = heapq.heappop(self._scheduled)
                self._ready.append((cb, args))  # 符合要求的定时任务加入就绪队列

        # 把就绪的任务拿出来执行
        num = len(self._ready)
        for i in range(num):
            cb, args = self._ready.popleft()
            cb(*args)


if __name__ == "__main__":

    loop = EventLoop()
    for i in range(3):
        t = Task(main_task())
        
    loop.call_later(1, loop.stop)
    loop.run_forever()
