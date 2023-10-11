from multiprocessing import Pool, Manager
from collections import defaultdict
from typing import Callable, Dict, List
from enum import Enum
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

    def apply_async(self, func, args=(), kwds={}, callback=None, error_callback=None):
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
    
    def results(self):
        res = []
        for idx in range(len(self.future_list)):
            if self.status_list[idx].status == Status.FINISHED and not self.result_fetched[idx]:
                self.result_list[idx] = self.future_list[idx].get()
                self.result_fetched[idx] = True
            if self.status_list[idx].status == Status.FINISHED:
                res.append(self.result_list[idx])
        return res

    def monitor(self):
        monitor = RichPoolMonitor(self)
        ret_value = monitor.run()
        if ret_value == 1:
            try:
                import IPython

                IPython.embed()
            except ModuleNotFoundError:
                print("IPython import failed, use `code`")
                import code

                local_vars = locals()
                shell = code.InteractiveConsole(local_vars)
                shell.interact()
        

class RichPoolMonitor(App):
    CSS_PATH = "rich_pool_monitor.tcss"
    BINDINGS = [
        ("r", "refresh_table", "Refresh data table"), 
        ("q", "quit", "Quit"),
        ("i", "interactive", "Spawn Interactive Shell"),
        ("k", "kill_process", "Kill process"),
        ("ctrl+k", "kill_all_process", "Kill all process"),
    ]
    
    def __init__(self, pool: RichPool):
        super().__init__()
        
        self.pool = pool
        
        # Init data_tables
        self.data_tables = {}
        for status in Status:
            dt = DataTable(id=status.name)
            dt.cursor_type = "row"
            self.data_tables[status] = {
                "data_table": dt,
                "keys": []
            }
            self.load_dt(status)
        
        # Init progress_bar
        self.progress_bar = ProgressBar(total = 10000)
        
        # Init status_banner
        self.status_banner = Label("Not Started Yet")
        
    
    def load_dt(self, _status: ProcStatus) -> None:
        status_list = self.pool.status_list
        content = []
        for idx in range(len(status_list)):
            status = status_list[idx]
            if status.status == _status:
                row = {}
                row.update(
                    {"index": idx, "pid": status.pid, "args": status.args, "kwargs": status.kwargs, "time": status.time}
                )
                row.update(status.content)
                for key in row.keys():
                    if key not in self.data_tables[_status]["keys"]:
                        self.data_tables[_status]["keys"].append(key)
                        self.data_tables[_status]["data_table"].add_column(key)
                content.append(row)
        self.data_tables[_status]["data_table"].clear()
        for row in content:
            to_add = [row.get(key, "") for key in self.data_tables[_status]["keys"]]
            self.data_tables[_status]["data_table"].add_row(*to_add, key=str(row["index"]))
    
    async def on_idle(self, event: events.Idle) -> None:
        count = self.pool.count_status()
        total = len(self.pool.status_list)
        completed = count[Status.ERROR] + count[Status.TERMINATED] + count[Status.FINISHED]
        self.progress_bar.update(total = total, progress=completed)
        self.status_banner.update(f"{count[Status.WAITING]}/{total} waiting, {count[Status.RUNNING]}/{total} running, {count[Status.ERROR]}/{total} error, {count[Status.TERMINATED]} terminated, {count[Status.FINISHED]} finished")
        await sleep(0.5)
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.query_one(ContentSwitcher).current = event.button.id
    
    def compose(self) -> ComposeResult:
        with Vertical():
            yield self.progress_bar
            yield self.status_banner
            with Horizontal():
                with Vertical():
                    for status in Status:
                        yield Button(status.name, id=status.name)
                with ContentSwitcher(initial=Status.RUNNING.name, id="switcher"):
                    for status in Status:
                        yield self.data_tables[status]["data_table"]
            yield Footer()
    
    def action_refresh_table(self) -> None:
        for status in Status:
            self.load_dt(status)       
            self.data_tables[status]["data_table"].refresh()
    
    def action_quit(self) -> None:
        self.exit(0)
    
    def action_interactive(self) -> None:
        self.exit(1)
    
    def action_kill_process(self) -> None:
        if self.query_one(ContentSwitcher).current == Status.RUNNING.name:
            dt: DataTable = self.data_tables[Status.RUNNING]["data_table"]
            row = dt.get_row_at(dt.cursor_row)
            index = row[0]
            self.pool.kill_process(index)
        self.action_refresh_table()
    
    def action_kill_all_process(self) -> None:
        self.pool.kill_all()
        self.action_refresh_table()
