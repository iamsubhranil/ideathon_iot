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
        return False

    def list_groups(self):
        groups = storage.execute_query("select * from groups")
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("ID", width=12)
        table.add_column("Name", width=12)
        table.add_column("Edge nodes", width=12)
        table.add_column("Devices", width=12)
        for group in groups:
            nodes = storage.execute_query(
                "select count(*) from edgenode where group_id = ?", (group[0],))[0][0]
            devices = storage.execute_query(
                "select count(*) from device where edge_node_id in "
                "(select edge_node_id from edgenode where group_id = ?)", (group[0],))[0][0]
            table.add_row(str(group[0]), group[1], str(nodes), str(devices))
        self.console.print(table)

    def list_nodes(self):
        nodes = storage.execute_query("select * from edgenode")
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("ID", width=12)
        table.add_column("Name", width=12)
        table.add_column("Group", width=12)
        table.add_column("Devices", width=12)
        table.add_column("Status", width=12)
        for node in nodes:
            group = storage.execute_query(
                "select group_name from groups where group_id = ?", (node[1],))[0][0]
            devices = storage.execute_query(
                "select count(*) from device where edge_node_id = ?", (node[0],))[0][0]
            table.add_row(str(node[0]), node[2],
                          group, str(devices), node[3])
        self.console.print(table)

    def list_devices(self):
        devices = storage.execute_query("select * from device")
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("ID", width=12)
        table.add_column("Name", width=12)
        table.add_column("Group", width=12)
        table.add_column("Node", width=12)
        table.add_column("Status", width=12)
        for device in devices:
            node, group = storage.execute_query(
                "select edge_node_name, group_id from edgenode where edge_node_id = ?", (device[1],))[0]
            group = storage.execute_query(
                "select group_name from groups where group_id = ?", (group,))[0][0]
            table.add_row(str(device[0]), device[2],
                          str(group), str(node), device[3])
        self.console.print(table)

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
                            value[0][1]) + ", timestamp=" + str(value[0][2]) + ")") if len(value) > 0 else ")"))
                        branch_device.add(
                            metric_label, guide_style="tree.line")
            self.console.print(branch_group)

    def show_error(self, message, command):
        self.console.print(message + "!", style="bold red")
        self.console.print("Try [b]'help " + command +
                           "'[/b] for more information.")

    def print_help_text(self, text):
        lines = text.splitlines()
        summary = lines[1]
        self.console.print(summary, style="italic green")
        usage = lines[2].split(" ")
        self.console.print(
            "\n[u]Usage:[/u]\n\t[b]" + usage[0] + "[/b]", end=" ")
        for part in usage[1:]:
            self.console.print(part, style="italic white", end=" ")
        self.console.print("\n[u]Details:[/u]")
        for line in lines[3:]:
            self.console.print(line)

    def help_get(self):
        self.print_help_text(self.do_get.__doc__)

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
                self.list_devices()
            else:
                self.show_error("Unknown category " + parts[0], "get")
        else:
            self.list_all()
