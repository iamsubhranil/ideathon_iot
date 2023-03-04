import storage
import cmd
from rich import print, pretty
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.text import Text
from rich.live import Live

import time


def unix_time_diff_to_string(time1):
    unix_time_diff = int(time.time() - time1)
    if unix_time_diff < 60:
        return str(unix_time_diff) + " seconds"
    elif unix_time_diff < 3600:
        return str(unix_time_diff // 60) + " minutes"
    elif unix_time_diff < 86400:
        return str(unix_time_diff // 3600) + " hours"
    else:
        return str(unix_time_diff // 86400) + " days"


class SparkplugREPL(cmd.Cmd):

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.prompt = "[iot] "
        self.console = Console()
        pretty.install()

    def do_exit(self, line):
        return False

    def list_groups(self):
        groups = storage.get_groups()
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("ID", width=12)
        table.add_column("Name", width=12)
        table.add_column("Edge nodes", width=12)
        table.add_column("Devices", width=12)
        for group in groups:
            nodes = storage.get_edge_node_count(group[0])
            devices = storage.get_device_count_by_group(group[0])
            table.add_row(str(group[0]), group[1], str(nodes), str(devices))
        self.console.print(table)

    def list_nodes(self):
        nodes = storage.get_edge_nodes()
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("ID", width=12)
        table.add_column("Name", width=12)
        table.add_column("Group", width=12)
        table.add_column("Devices", width=12)
        table.add_column("Status", width=12)
        for node in nodes:
            group = storage.get_group_name(node[1])
            devices = storage.get_device_count_by_edge_node(node[0])
            table.add_row(str(node[0]), node[2],
                          group, str(devices), node[3])
        self.console.print(table)

    def list_all_devices(self):
        devices = storage.get_devices()
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("ID", width=12)
        table.add_column("Name", width=12)
        table.add_column("Group", width=12)
        table.add_column("Node", width=12)
        table.add_column("Status", width=12)
        for device in devices:
            node = storage.get_edge_node_name(device[1])
            group = storage.get_group_name_by_edge_node(device[1])
            table.add_row(str(device[0]), device[2],
                          str(group), str(node), device[3])
        self.console.print(table)

    def list_devices(self, device):
        if len(device) > 0:
            parts = device[0].split("/")
            group, node, device = None, None, None
            if len(parts) == 3:
                group, node, device = parts
            elif len(parts) == 2:
                node, device = parts
            else:
                device = parts[0]
            devices = storage.get_device_by_name(
                group, node, device)
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("ID", width=12)
            table.add_column("Name", width=12)
            table.add_column("Group", width=12)
            table.add_column("Node", width=12)
            table.add_column("Status", width=12)
            table.add_column(
                "Metrics (Name, Value, Last Updated)", no_wrap=True)
            for device in devices:
                node = storage.get_edge_node_name(device[1])
                group = storage.get_group_name_by_edge_node(device[1])
                metrics = storage.get_metrics_by_device(device[0])
                metrics_table = Table(show_header=False)
                metrics_table.add_column("Name")
                metrics_table.add_column("Value")
                metrics_table.add_column("Updated")
                for metric in metrics:
                    metrics_table.add_row(metric[0], str(metric[2]),
                                          unix_time_diff_to_string(metric[3]))
                    metrics_table.add_section()
                table.add_row(str(device[0]), device[2],
                              str(group), str(node), device[3], metrics_table)
                table.add_section()
            self.console.print(table)
        else:
            self.list_all_devices()

    def list_all(self):
        groups = storage.execute_query("select * from groups")
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
SORT
    def do_get(self, line):
        """ 
Get information about a specific group, node or device.
get <group|node|device> <id|name>
If no id or name is provided, all members of that category will be listed.
If no cateogry is provided, all groups, nodes and devices will be listed
in a tree view.
If there are multiple matches for a name, all of them will be listed.
        """
        if line != "":
            parts = line.split(" ")
            if parts[0] == "group":
                self.list_groups()
            elif parts[0] == "node":
                self.list_nodes()
            elif parts[0] == "device":
                self.list_devices(parts[1:])
            else:
                self.show_error("Unknown category " + parts[0], "get")
        else:
            self.list_all()
