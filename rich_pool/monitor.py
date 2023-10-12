from textual.widgets import DataTable, Footer, ProgressBar, Label, Button, ContentSwitcher
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual import events
from typing import Dict, List, Any
from asyncio import sleep

from .status import Status, ProcStatus



class RichPoolMonitor(App):
    CSS_PATH = "rich_pool_monitor.tcss"
    BINDINGS = [
        ("r", "refresh_table", "Refresh data table"), 
        ("q", "quit", "Quit"),
        ("i", "interactive", "Spawn Interactive Shell"),
        ("k", "kill_process", "Kill process"),
        ("ctrl+k", "kill_all_process", "Kill all process"),
    ]
    
    def __init__(self, pool):
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
