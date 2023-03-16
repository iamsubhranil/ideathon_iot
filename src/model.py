import storage
from typing import Any
import math
import statistics

# An object that can be queried from the storage backend
#
# For each of the gettable attributes, the storage backend
# will need to implement the following method:
# def get(device_type, device_id, attribute_name)
#
# For each of the settable attributes, the storage backend
# will need to implement the following method:
# def set(device_type, device_id, attribute_name, attribute_value)


class Queryable(object):
    # name -> type of the object
    # id -> id of the object
    # attributes -> dict of attribute:can_be_cached
    # settable -> list of attributes that can be set
    def __init__(self, name: str, id: int, attributes: dict, settable: list[str] = []):
        self.__dict__["attributes"] = attributes.keys()
        self.__dict__["can_be_cached"] = attributes
        self.__dict__["cached"] = {}
        self.__dict__["type_name"] = name
        self.__dict__["id"] = id
        self.__dict__["settable"] = settable
        self.__dict__["special_types"] = {"group": Group, "node": Node,
                                          "device": Device, "metric": Metric}

    # Dynamically get attributes from the storage backend
    def __getattr__(self, attribute):
        # Check if the attribute exists
        if attribute not in self.attributes:
            raise AttributeError(
                self.type_name + " has no attribute " + attribute)
        # Check if the attribute is cached
        if attribute in self.cached:
            return self.cached[attribute]
        # Get the attribute from the storage backend
        value = storage.get(
            self.type_name.lower(), self.id, attribute)
        # Convert the value to the correct type if required
        if attribute in self.special_types:
            value = self.special_types[attribute](value)
        elif attribute.endswith("s") and attribute[:-1] in self.special_types:
            value = [self.special_types[attribute[:-1]](
                item_id) for item_id in value]

        # Cache the value if required
        if self.can_be_cached[attribute]:
            self.cached[attribute] = value

        return value

    # Dynamically set attributes in the storage backend
    def __setattr__(self, __name: str, __value: Any) -> None:
        # Check if the attribute declared as settable
        if __name in self.settable:
            storage.set(self.type_name.lower(), self.id, __name, __value)
        else:
            super().__setattr__(__name, __value)

    # Convert the object to a string
    def __str__(self):
        return self.type_name + "<id=" + str(self.id) + ",name=" + str(self.name) + ">"

    def __repr__(self):
        return self.__str__()


# A metric that can be queried by name
# For example, device1.metric.metric1.value
class NamedMetric(object):
    def __init__(self, device):
        self.device = device

    def __getattr__(self, attribute):
        # Check if the metric exists
        for metric in self.device.metrics:
            # If the name matches, return the metric
            if metric.name == attribute:
                return metric
        # If no metric was found, raise an error
        raise AttributeError(
            "Device " + self.device.name + " has no metric " + attribute)

    def has(self, metric):
        try:
            self.__getattr__(metric)
            return True
        except AttributeError:
            return False

    def get(self, metric):
        return self.__getattr__(metric)


# A device metric that provides some fields to query
# Gettable fields: name, type, value, timestamp, values
# Settable fields: value
class Metric(Queryable):
    def __init__(self, id):
        super().__init__("Metric", id, {
            "name": True, "type": True, "value": False, "timestamp": False, "values": False},
            ["value"])


# A device that can be queried by name
class NamedDevice(object):
    def __init__(self, node):
        self.node = node

    def __getattr__(self, attribute):
        # Check if the device exists
        for device in self.node.devices:
            if device.name == attribute:
                return device
        # If no device was found, raise an error
        raise AttributeError(
            "Node " + self.node.name + " has no device " + attribute)


# A device that provides some fields to query
# Gettable fields: name, group, node, status, metrics, birth_timestamp, death_timestamp
# Settable fields: status
class Device(Queryable):
    def __init__(self, id):
        super().__init__("Device", id, {
            "name": True, "group": True, "node": True, "status": False, "metrics": True,
            "birth_timestamp": False, "death_timestamp": False, "metric": False},
            ["status", "death_timestamp", "birth_timestamp"])

    def __getattr__(self, attribute):
        if attribute == "metric":
            return NamedMetric(self)
        return super().__getattr__(attribute)


# A node that can be queried by name
# For example, group1.node.node1.status
class NamedNode(object):
    def __init__(self, group):
        self.group = group

    def __getattr__(self, attribute):
        for node in self.group.nodes:
            if node.name == attribute:
                return node
        raise AttributeError(
            "Group " + self.group.name + " has no node " + attribute)


# A node that provides some fields to query
# Gettable fields: name, group, status, devices, birth_timestamp, death_timestamp
# Settable fields: status
class Node(Queryable):
    def __init__(self, id):
        super().__init__("Node", id, {
            "name": True, "group": True, "status": False, "devices": False,
            "birth_timestamp": False, "death_timestamp": False, "device": False},
            ["status", "death_timestamp", "birth_timestamp"])

    def __getattr__(self, attribute):
        if attribute == "device":
            return NamedDevice(self)
        return super().__getattr__(attribute)


# A group that provides some fields to query
# Gettable fields: name, nodes, devices, node
# Settable fields: None
class Group(Queryable):
    def __init__(self, id):
        super().__init__("Group", id, {
            "name": True, "nodes": False, "devices": False, "node": False},
            ["name"])

    def __getattr__(self, attribute):
        if attribute == "node":
            return NamedNode(self)
        return super().__getattr__(attribute)


# Return a list of groups matching the given naming pattern
def get_group(name) -> list[Group]:
    groups = storage.get_group_by_name(name)
    if len(groups) == 0:
        raise ValueError("No such group found!")
    return [Group(group_id) for group_id in groups]


# Return a list of nodes matching the given naming pattern
def get_node(group_name, node_name) -> list[Node]:
    nodes = storage.get_node_by_name(group_name, node_name)
    if len(nodes) == 0:
        raise ValueError("No such node found!")
    return [Node(node_id) for node_id in nodes]


# Return a list of devices matching the given naming pattern
def get_device(group_name, node_name, device_name) -> list[Device]:
    devices = storage.get_device_by_name(
        group_name, node_name, device_name)
    if len(devices) == 0:
        full_name = ""
        full_name += (group_name + "/") if group_name else ""
        full_name += (node_name + "/") if node_name else ""
        full_name += device_name

        raise ValueError("No such device found: " + full_name)
    return [Device(device_id) for device_id in devices]


# Return a list of all groups
def get_groups() -> list[Group]:
    return [Group(group_id) for group_id in storage.get_all_groups()]


# Return a list of all nodes
def get_nodes() -> list[Node]:
    return [Node(node_id) for node_id in storage.get_all_nodes()]


# Return a list of all devices
def get_devices() -> list[Device]:
    return [Device(device_id) for device_id in storage.get_all_devices()]


# Return a specific group, node, or device
# If multiple matches are found, return a list of them
def get(name):
    value = []
    # If the name contains a slash, it is a group/node/device name
    if name.find("/") != -1:
        names = name.split("/")
        # If the name contains 3 slashes, it is a device name
        if len(names) == 3:
            value = get_device(names[0], names[1], names[2])
        # If the name contains 2 slashes, it is a node/device name
        elif len(names) == 2:
            try:
                value = get_device("", names[0], names[1])
            except:
                pass
            try:
                value += get_node(names[0], names[1])
            except:
                pass
    # If the name contains no slashes, it can be anything
    # So we try to find a group, node, or device with that name
    else:
        values = []
        try:
            values += get_device("", "", name)
        except:
            pass
        try:
            values += get_node("", name)
        except:
            pass
        try:
            values += get_group(name)
        except:
            pass
        value = values
    if len(value) == 1:
        return value[0]
    elif len(value) > 1:
        return value
    else:
        raise ValueError("No such name: " + name)


# Create a new group, returns the new group
def create_group(name):
    group_id = storage.insert_group(name)
    return Group(group_id)


# Create a new node, returns the new node
def create_node(group_name, node_name):
    node_id = storage.insert_node(group_name, node_name, "NA", 0, 0)
    return Node(node_id)


# Create a new device, returns the new device
def create_device(group_name, node_name, device_name):
    device_id = storage.insert_device(
        group_name, node_name, device_name, "NA", 0, 0)
    return Device(device_id)


# Create a new metric, returns the new metric
def create_metric(group_name, node_name, device_name, metric_name, metric_type):
    metric_id = storage.insert_metric(group_name, node_name,
                                      device_name, metric_name, metric_type)
    return Metric(metric_id)


# Starts the storage system
def startup():
    storage.startup()


# Shuts down the storage system
def shutdown():
    storage.shutdown()


# The dictionary of functions that can be called from the 'expr'
# in the CLI
RUNTIME_DICT = {
    "math": math,
    "statistics": statistics,
    "get": get,
    "get_group": get_group,
    "get_node": get_node,
    "get_device": get_device,
    "get_groups": get_groups,
    "get_nodes": get_nodes,
    "get_devices": get_devices,
    "Node": Node,
    "Group": Group,
    "Device": Device,
    "Metric": Metric,
}
