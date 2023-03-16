import cmd
import time
import model
import re
from rich import print, pretty
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.text import Text
from rich.live import Live


# Convert a unix time difference to a string
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


# Create a rich table
def create_table(cols, header_style=None, show_header=True):
    table = Table(show_header=show_header, header_style=header_style)
    for col in cols:
        table.add_column(col, no_wrap=True)
    return table


# Base class for the REPL
class SparkplugREPL(cmd.Cmd):

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.prompt = "=> "
        self.console = Console()
        pretty.install()

    # Exit the REPL
    def do_exit(self, line):
        """
Exit from the REPL.
exit
        """
        raise KeyboardInterrupt

    # Show an error message
    def show_error(self, message, command):
        self.console.print(message + "!", style="bold red")
        self.console.print("Try [b]'help " + command +
                           "'[/b] for more information.")

    # Print a formatted help text for a command

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

    # Show help for a command
    def help_exit(self):
        self.print_help_text(self.do_exit.__doc__)

    def help_get(self):
        self.print_help_text(self.do_get.__doc__)

    def help_watch(self):
        self.print_help_text(self.do_watch.__doc__)

    def help_expr(self):
        self.print_help_text(self.do_expr.__doc__)

    def help_assign(self):
        self.print_help_text(self.do_assign.__doc__)

    # List all groups in the system
    def list_groups(self):
        groups = model.get_groups()
        table = create_table(
            ["ID", "Name", "Edge nodes", "Devices"], "bold magenta")
        for group in groups:
            nodes = len(group.nodes)
            devices = len(group.devices)
            table.add_row(str(group.id), group.name, str(nodes), str(devices))
        self.console.print(table)

    # List all nodes in the system
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

    # List all devices in the system
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

    # Generate a detailed overview of a device
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

    # List all devices in the system
    def list_devices(self, device):
        if len(device) > 0:
            self.console.print(self.generate_device_details(device))
        else:
            self.list_all_devices()

    # List the system topology
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

    # Geet information about a specific group, node or device
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
            try:
                if parts[0] == "group":
                    self.list_groups()
                elif parts[0] == "node":
                    self.list_nodes()
                elif parts[0] == "device":
                    self.list_devices(parts[1:])
                else:
                    self.show_error("Unknown category " + parts[0], "get")
            except:
                self.show_error("Unable to find item", "get")
        else:
            self.list_all()

    # Watch a specific device for live changes
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

    def eval_expr(self, expr):
        # extract all @[a-zA-Z][a-zA-Z_0-9]* parts and replace
        # them with the corresponding value from runtime_dict
        # e.g. @a + @b will be replaced with 1 + 2
        # if runtime_dict = {"a": 1, "b": 2}
        values = re.findall(r'@[a-zA-Z][a-zA-Z_0-9]*', expr)
        for value in values:
            ident = value[1:]
            if ident not in model.RUNTIME_DICT:
                raise Exception("Identifier not defined: " + ident)
            expr = expr.replace(value, str(model.RUNTIME_DICT[ident]))
        return eval(expr, model.RUNTIME_DICT)

    # Evaluate an expression
    def do_expr(self, line):
        """
Evaluate an expression.
expr <expression>
You can evaluate any expression that is valid in Python.
To get the list of all devices, use get_devices().
To get the list of all nodes, use get_nodes().
To get the list of all groups, use get_groups().
To retrieve a specific device/node/group, use get("group1/node0/device1").
You can apply any transformation to the list of devices, nodes or groups.
For example,
    expr max(get("group1/node0/device1").metric.temperature.values)
        """
        if line == "":
            self.show_error("No expression provided", "expr")
            return
        try:
            e = self.eval_expr(line)
            self.console.print(e)
        except Exception as e:
            print(str(e.__class__.__name__) + ":", e)
            self.show_error("Error in expression", "expr")

    def do_assign(self, line):
        """
Assign a value to a variable.
assign <variable_name> <expression>
Expression will be evaluated following the same rules as expr.
        """
        if line == "":
            self.show_error("No expression provided", "assign")
            return
        parts = line.split(" ")
        if len(parts) < 2:
            self.show_error("No expression provided", "assign")
            return
        try:
            model.RUNTIME_DICT[parts[0]] = self.eval_expr(
                " ".join(parts[1:]))
        except Exception as e:
            print(str(e.__class__.__name__) + ":", e)
            self.show_error("Error in expression", "assign")

    def do_define(self, line):
        """
Assign an expression to a variable.
define <variable_name> <expression>
Expression will not be evaluated until the variable is used.
Use @ to access the variable.
For example,
    define temp get("group1/node0/device1").metric.temperature.values
    expr max(@temp)
        """
        if line == "":
            self.show_error("No expression provided", "define")
            return
        parts = line.split(" ")
        if len(parts) < 2:
            self.show_error("No expression provided", "define")
            return
        model.RUNTIME_DICT[parts[0]] = " ".join(parts[1:])


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
