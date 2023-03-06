import cmd
import time
import model
from rich import print, pretty
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.text import Text
from rich.live import Live


def unix_time_diff_to_string(time1):
    unix_time_diff = int(time.time() - time1)
    if unix_time_diff < 60:
        return str(unix_time_diff) + " second" + ("s" if unix_time_diff > 1 else "")
    elif unix_time_diff < 3600:
        return str(unix_time_diff // 60) + " minutes"
    elif unix_time_diff < 86400:
        return str(unix_time_diff // 3600) + " hours"
    else:
        return str(unix_time_diff // 86400) + " days"


def create_table(cols, header_style=None, show_header=True):
    table = Table(show_header=show_header, header_style=header_style)
    for col in cols:
        table.add_column(col, no_wrap=True)
    return table


class SparkplugREPL(cmd.Cmd):

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.prompt = "=> "
        self.console = Console()
        pretty.install()

    def do_exit(self, line):
        """
Exit from the REPL.
exit
        """
        raise KeyboardInterrupt

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

    def help_exit(self):
        self.print_help_text(self.do_exit.__doc__)

    def help_get(self):
        self.print_help_text(self.do_get.__doc__)

    def help_watch(self):
        self.print_help_text(self.do_watch.__doc__)

    def list_groups(self):
        groups = model.get_groups()
        table = create_table(
            ["ID", "Name", "Edge nodes", "Devices"], "bold magenta")
        for group in groups:
            nodes = len(group.nodes)
            devices = len(group.devices)
            table.add_row(str(group.id), group.name, str(nodes), str(devices))
        self.console.print(table)

    def list_nodes(self):
        nodes = model.get_nodes()
        table = create_table(
            ["ID", "Name", "Group", "Devices", "Status"], "bold magenta")
        for node in nodes:
            group = node.group.name
            devices = len(node.devices)
            table.add_row(str(node.id), node.name,
                          group, str(devices), node.status)
        self.console.print(table)

    def list_all_devices(self):
        devices = model.get_devices()
        table = create_table(
            ["ID", "Name", "Group", "Node", "Status"], "bold magenta")
        for device in devices:
            node = device.node.name
            group = device.group.name
            table.add_row(str(device.id), device.name,
                          str(group), str(node), device.status)
        self.console.print(table)

    def generate_device_details(self, device):
        parts = device[0].split("/")
        group, node, device = None, None, None
        if len(parts) == 3:
            group, node, device = parts
        elif len(parts) == 2:
            node, device = parts
        else:
            device = parts[0]
        devices = model.get_device(group, node, device)
        table = create_table(["ID", "Name", "Group", "Node", "Status",
                             "Metrics (Name, Value, Last Updated)"], "bold magenta")
        for device in devices:
            node = device.node.name
            group = device.group.name
            metrics = device.metrics
            metrics_table = create_table(
                ["Name", "Value", "Updated"], show_header=False)
            for metric in metrics:
                value = metric.value
                if metric.type == "string":
                    value = "'" + value + "'"
                elif metric.type == "boolean":
                    value = str(value).lower()
                elif metric.type == "float":
                    value = "{:.2f}".format(value)
                metrics_table.add_row(metric.name, str(metric.value),
                                      unix_time_diff_to_string(metric.timestamp))
                metrics_table.add_section()
            table.add_row(str(device.id), device.name,
                          str(group), str(node), device.status, metrics_table)
            table.add_section()
        return table

    def list_devices(self, device):
        if len(device) > 0:
            self.console.print(self.generate_device_details(device))
        else:
            self.list_all_devices()

    def list_all(self):
        groups = model.get_groups()
        for group in groups:
            branch_group = Tree(
                Text(group.name, style="bold blue"), guide_style="bold blue")
            nodes = group.nodes
            for node in nodes:
                branch_node = branch_group.add(
                    Text(node.name, style="bold green"), guide_style="bold green")
                devices = node.devices
                for device in devices:
                    branch_device = branch_node.add(
                        Text(device.name, style="bold white") + " (id=" + str(device.id) + ")", guide_style="bold white")
                    metrics = device.metrics
                    for metric in metrics:
                        metric_label = Text(metric.name + " (type=" + metric.type + (", value=" + str(
                            metric.value) + ", timestamp=" + str(metric.timestamp) + ")"))
                        branch_device.add(
                            metric_label, guide_style="tree.line")
            self.console.print(branch_group)

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

    def do_watch(self, line):
        """
Watch a specific device for live changes.
watch <device_name>
If multiple devices match the name, all of them will be watched.
Press Ctrl+C to exit the live view.
        """
        if line == "":
            self.show_error("No device name provided", "watch")
            return
        with Live(self.generate_device_details([line]), refresh_per_second=1) as live:
            while True:
                try:
                    time.sleep(1)
                    live.update(self.generate_device_details([line]))
                except KeyboardInterrupt:
                    break

    def do_expr(self, line):
        """
Evaluate an expression.
expr <expression>
You can evaluate any expression that is valid in Python.
To get the list of all devices, use model.get_devices().
To get the list of all nodes, use model.get_nodes().
To get the list of all groups, use model.get_groups().
You can apply any transformation to the list of devices, nodes or groups.
For example,
    expr max(model.get_device("group1", "node1", "device1")[0].metric.temperature.values)
        """
        if line == "":
            self.show_error("No expression provided", "expr")
            return
        try:
            e = eval(line, model.__dict__)
            self.console.print(e)
        except Exception as e:
            print(str(e.__class__.__name__) + ":", e)
            self.show_error("Error in expression", "expr")


def main():
    model.startup()
    while True:
        try:
            SparkplugREPL().cmdloop()
        except KeyboardInterrupt:
            model.shutdown()
            break


if __name__ == "__main__":
    main()
