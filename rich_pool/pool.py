from multiprocessing import Pool, Manager
from collections import defaultdict
from typing import Callable, Dict, List

import os 
import signal

from textual.widgets import DataTable, Footer, ProgressBar, Label, Button, ContentSwitcher
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual import events
from typing import Dict, List, Any
from asyncio import sleep

import time
import random

from .status import ProcStatus, Status
from .monitor import RichPoolMonitor, RichPoolExitResult


def wrap_function(
    func: Callable,
    origin_args: tuple,
    origin_kwargs: dict,
    status: ProcStatus,
    pass_status: bool,
):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    assert isinstance(status, ProcStatus)
    assert status.status == Status.WAITING

    status.pid = os.getpid()
    status.status = Status.RUNNING
    status.time_start()
    try:
        if pass_status:
            result = func(status, *origin_args, **origin_kwargs)
        else:
            result = func(*origin_args, **origin_kwargs)
        status.status = Status.FINISHED
    except Exception:
        status.status = Status.ERROR
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        raise

    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if status.status == Status.FINISHED:
        return result
    else:
        return None


class RichPool:
    def __init__(
        self,
        num_processes=os.cpu_count(),
        pass_status: bool = False,
    ):
        self.pass_status = pass_status
        self.num_processes = num_processes

        self.pool = Pool(num_processes)
        self.manager = Manager()
        self.status_list = []
        self.future_list = []
        self.result_list = []
        self.result_fetched = []

    def apply_async(self, func, args=(), kwds={}, callback: Callable = None, error_callback: Callable = None):
        status = ProcStatus(self.manager)
        status.args = args
        status.kwargs = kwds
        self.status_list.append(status)
        self.result_list.append(None)
        self.result_fetched.append(False)
        args = (func, args, kwds, status, self.pass_status)
        self.future_list.append(
            self.pool.apply_async(
                func=wrap_function,
                args=args,
                callback=callback,
                error_callback=error_callback,
            )
        )

    def close(self):
        self.pool.close()

    def join(self):
        self.pool.join()

    def count_status(self) -> Dict[Status, int]:
        res = defaultdict(lambda: 0)
        for status in self.status_list:
            res[status.status] += 1
        return res

    def kill_process(self, idx: int):
        pid = self.status_list[idx].pid
        if pid == -1 or self.status_list[idx].status != Status.RUNNING:
            return
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass
        self.status_list[idx].status = Status.TERMINATED
    
    def kill_all(self):
        self.pool.terminate()
        for idx in range(len(self.status_list)):
            if self.status_list[idx].status == Status.RUNNING or self.status_list[idx].status == Status.WAITING:
                self.status_list[idx].status = Status.TERMINATED
    
    def results(self, only_new: bool = False):
        res = []
        new_res = []
        for idx in range(len(self.future_list)):
            if self.status_list[idx].status == Status.FINISHED and not self.result_fetched[idx]:
                self.result_list[idx] = self.future_list[idx].get()
                self.result_fetched[idx] = True
                new_res.append(self.result_list[idx])
            if self.status_list[idx].status == Status.FINISHED:
                res.append(self.result_list[idx])
        if only_new:
            return new_res
        else:
            return res

    def monitor(self):
        monitor = RichPoolMonitor(self)
        ret_value = monitor.run()
        if ret_value == RichPoolExitResult.INTERACTIVE:
            try:
                import IPython

                IPython.embed()
            except ModuleNotFoundError:
                print("IPython import failed, use `code`")
                import code

                local_vars = locals()
                shell = code.InteractiveConsole(local_vars)
                shell.interact()
        
        return ret_value