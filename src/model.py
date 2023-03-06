import storage
from typing import Any


class Queryable(object):
    # name -> type of the object
    # id -> id of the object
    # attributes -> dict of attribute:can_be_cached
    def __init__(self, name: str, id: int, attributes: dict, settable: list[str] = []):
        self.__dict__["attributes"] = attributes.keys()
        self.__dict__["can_be_cached"] = attributes
        self.__dict__["cached"] = {}
        self.__dict__["type_name"] = name
        self.__dict__["id"] = id
        self.__dict__["settable"] = settable
        self.__dict__["special_types"] = {"group": Group, "node": Node,
                                          "device": Device, "metric": Metric}

    def __getattr__(self, attribute):
        if attribute not in self.attributes:
            raise AttributeError(
                self.type_name + " has no attribute " + attribute)
        if attribute in self.cached:
            return self.cached[attribute]
        value = storage.get(
            self.type_name.lower(), self.id, attribute)
        if attribute in self.special_types:
            value = self.special_types[attribute](value)
        elif attribute.endswith("s") and attribute[:-1] in self.special_types:
            value = [self.special_types[attribute[:-1]](
                item_id) for item_id in value]

        if self.can_be_cached[attribute]:
            self.cached[attribute] = value

        return value

    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name in self.settable:
            storage.set(self.type_name.lower(), self.id, __name, __value)
        else:
            super().__setattr__(__name, __value)

    def __str__(self):
        return "<" + self.type_name + "#" + str(self.id) + ">"


class NamedMetric(object):
    def __init__(self, device):
        self.device = device

    def __getattr__(self, attribute):
        for metric in self.device.metrics:
            if metric.name == attribute:
                return metric
        raise AttributeError(
            "Device " + self.device.name + " has no metric " + attribute)


class Metric(Queryable):
    def __init__(self, id):
        super().__init__("Metric", id, {
            "name": True, "type": True, "value": False, "timestamp": False, "values": False},
            ["value"])


class NamedDevice(object):
    def __init__(self, node):
        self.node = node

    def __getattr__(self, attribute):
        for device in self.node.devices:
            if device.name == attribute:
                return device
        raise AttributeError(
            "Node " + self.node.name + " has no device " + attribute)


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


class NamedNode(object):
    def __init__(self, group):
        self.group = group

    def __getattr__(self, attribute):
        for node in self.group.nodes:
            if node.name == attribute:
                return node
        raise AttributeError(
            "Group " + self.group.name + " has no node " + attribute)


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


class Group(Queryable):
    def __init__(self, id):
        super().__init__("Group", id, {
            "name": True, "nodes": False, "devices": False, "node": False},
            ["name"])

    def __getattr__(self, attribute):
        if attribute == "node":
            return NamedNode(self)
        return super().__getattr__(attribute)


def get_group(name) -> list[Group]:
    groups = storage.get_group_by_name(name)
    if len(groups) == 0:
        raise ValueError("No such group found!")
    return [Group(group_id[0]) for group_id in groups]


def get_node(group_name, node_name) -> list[Node]:
    nodes = storage.get_edge_node_by_name(group_name, node_name)
    if len(nodes) == 0:
        raise ValueError("No such node found!")
    return [Node(node_id[0]) for node_id in nodes]


def get_device(group_name, node_name, device_name) -> list[Device]:
    devices = storage.get_device_by_name(
        group_name, node_name, device_name)
    if len(devices) == 0:
        raise ValueError("No such device found: " +
                         group_name + "/" + node_name + "/" + device_name)
    return [Device(device_id[0]) for device_id in devices]


def get_groups() -> list[Group]:
    return [Group(group_id[0]) for group_id in storage.get_all_groups()]


def get_nodes() -> list[Node]:
    return [Node(node_id[0]) for node_id in storage.get_all_edge_nodes()]


def get_devices() -> list[Device]:
    return [Device(device_id[0]) for device_id in storage.get_all_devices()]


def get(name):
    value = []
    if name.find("/") != -1:
        names = name.split("/")
        if len(names) == 3:
            value = get_device(names[0], names[1], names[2])
        elif len(names) == 2:
            try:
                value = get_device("", names[0], names[1])
            except:
                value = get_node(names[0], names[1])
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


def create_group(name):
    group_id = storage.insert_group(name)
    return Group(group_id)


def create_node(group_name, node_name):
    node_id = storage.insert_edge_node(group_name, node_name, "NA", 0, 0)
    return Node(node_id)


def create_device(group_name, node_name, device_name):
    device_id = storage.insert_device(
        group_name, node_name, device_name, "NA", 0, 0)
    return Device(device_id)


def create_metric(group_name, node_name, device_name, metric_name, metric_type):
    metric_id = storage.insert_metric(group_name, node_name,
                                      device_name, metric_name, metric_type)
    return Metric(metric_id)


def startup():
    storage.startup()


def shutdown():
    storage.shutdown()
