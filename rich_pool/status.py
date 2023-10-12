from enum import Enum
import time
from multiprocessing import Manager


class Status(Enum):
    RUNNING = 1
    WAITING = 2
    ERROR = 3
    FINISHED = 4
    TERMINATED = 5


class ProcStatus:
    def __init__(self, manager: Manager):
        self._builtin_status = manager.dict()
        self.content = manager.dict()
        self._builtin_status["status"] = Status.WAITING
        self._builtin_status["pid"] = -1
        self._builtin_status["args"] = []
        self._builtin_status["kwargs"] = {}
        self._builtin_status["start_time"] = -1

    def __repr__(self) -> str:
        return str({"builtin": self._builtin_status, "user_define": self.content})

    def __getitem__(self, key):
        return self.content.get(key, None)

    def __setitem__(self, key, value):
        self.content[key] = value

    def update(self, val_dict: dict):
        self.content.update(val_dict)

    @property
    def status(self):
        return self._builtin_status["status"]

    @status.setter
    def status(self, value: Status):
        self._builtin_status["status"] = value

    @property
    def pid(self):
        return self._builtin_status["pid"]

    @pid.setter
    def pid(self, value):
        self._builtin_status["pid"] = value

    @property
    def args(self):
        return self._builtin_status["args"]

    @args.setter
    def args(self, value):
        self._builtin_status["args"] = value

    @property
    def kwargs(self):
        return self._builtin_status["kwargs"]

    @kwargs.setter
    def kwargs(self, value):
        self._builtin_status["kwargs"] = value
        
    @property
    def time(self):
        start = self._builtin_status["start_time"]
        if start < 0:
            return -1
        else:
            return time.time() - self._builtin_status["start_time"]
    
    def time_start(self):
        self._builtin_status["start_time"] = time.time()

