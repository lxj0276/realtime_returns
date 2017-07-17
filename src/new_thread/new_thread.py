
import time
import threading


class NewThread(threading.Thread):
    """ 能够暂停和终止的线程类 """
    def __init__(self,group=None, target=None, name=None, args=(), kwargs={}, daemon=None ,frequency=0.5):
        super(NewThread,self).__init__(group=group, name=name, daemon=daemon)
        self._target = target
        self._name = name
        self._args = args
        self._kwargs = kwargs
        self.frequency = frequency
        # 设置 event
        self.__onoff = threading.Event()
        self.__onoff.set()
        self.__gohold = threading.Event()
        self.__gohold.set()

    def run(self):  # run结束后进程结束
        while self.__onoff.is_set():
            self._target(*self._args,**self._kwargs)
            time.sleep(self.frequency)

    def pause(self):
        self.__gohold.clear()

    def resume(self):
        self.__gohold.set()

    def stop(self):
        self.__gohold.set()
        self.__onoff.clear()