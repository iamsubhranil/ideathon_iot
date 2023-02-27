import storage
import cmd
from rich import print, pretty
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.text import Text
from rich.live import Live


class SparkplugREPL(cmd.Cmd):

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.prompt = "[iot] "
        self.console = Console()
        pretty.install()

    def do_exit(self, line):
        return True

    def do_list(self, line):
        groups = storage.execute_query("select * from groups", ())
        for group in groups:
            branch_group = Tree(
                Text(group[1], style="bold blue"), guide_style="bold blue")
            nodes = storage.execute_query(
                "select * from edgenode where group_id = ?", (group[0],))
            for node in nodes:
                branch_node = branch_group.add(
                    Text(node[2], style="bold green"), guide_style="bold green")
                devices = storage.execute_query(
                    "select * from device where edge_node_id = ?", (node[0],))
                for device in devices:
                    branch_device = branch_node.add(
                        Text(device[2], style="bold white") + " (id=" + str(device[0]) + ")", guide_style="bold white")
                    metrics = storage.execute_query(
                        "select * from metric where device_id = ?", (device[0],))
                    for metric in metrics:
                        value = storage.execute_query(
                            "select * from Metric" + metric[3].capitalize() + " where metric_id = ? order by timestamp desc limit 1", (metric[0],))
                        metric_label = Text(metric[2] + " (type=" + metric[3] + ((", value=" + str(
                            value[0][1]) + ", timestamp=" + str(value[0][2]) + ")") if len(value) > 0 else ")"))
                        branch_device.add(
                            metric_label, guide_style="tree.line")
            self.console.print(branch_group)
