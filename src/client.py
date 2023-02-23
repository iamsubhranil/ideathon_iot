import paho.mqtt.client as mqtt
import time
import random

import sparkplug_b_pb2 as payload

KEEPALIVE = 60


def metric(payload1, alias, value, name=None, birth=False):
    metric = payload1.metrics.add()
    if alias:
        metric.alias = alias
    if birth or not alias:
        metric.name = name
    if type(value) == str:
        if birth:
            metric.datatype = payload.String
        metric.string_value = value
    elif type(value) == int:
        if birth:
            metric.datatype = payload.Int64
        metric.int_value = value
    elif type(value) == float:
        if birth:
            metric.datatype = payload.Float
        metric.float_value = value
    elif type(value) == bool:
        if birth:
            metric.datatype = payload.Boolean
        metric.boolean_value = value
    metric.timestamp = int(time.time())

# metrics is a list of name:value


def generate_metric(seq, metrics, mapping, birth=False, timestamp=None):
    payload1 = payload.Payload()
    if timestamp is None:
        timestamp = int(time.time())
    payload1.timestamp = timestamp
    payload1.seq = seq

    for name, value in metrics.items():
        metric(payload1, None if name not in mapping else mapping[name], value, name, birth)

    return payload1.SerializeToString()

def randomizer(values):
    return random.choice(values)

def integer_randomizer(min=0, max=100):
    return random.randint(min, max)

def float_randomizer(min=0.0, max=100.0):
    return random.uniform(min, max)

def boolean_randomizer():
    return random.choice([True, False])

class Device:

    def __init__(self, type, name, edgenode):
        self.type = type
        self.name = name
        self.edgenode = edgenode
        self.mapping = {}

        self.edgenode.register_device(self)

    def handle_message(self, client, userdata, msg):
        print("[" + self.name + "] RECV " +
              msg.topic + " " + str(msg.payload))


class Light(Device):

    def __init__(self, name, edgenode):
        super().__init__("light", name, edgenode)

    def get_metrics(self):
        return {"state": randomizer(["ON", "OFF"]), "brightness": integer_randomizer(0, 100)}


class TemperatureSensor(Device):

    def __init__(self, name, edgenode):
        super().__init__("temperature", name, edgenode)

    def get_metrics(self):
        return {"temperature": float_randomizer(0.0, 100.0), "unit": randomizer(["C", "F"])}


class HumiditySensor(Device):

    def __init__(self, name, edgenode):
        super().__init__("humidity", name, edgenode)

    def get_metrics(self):
        return {"humidity": integer_randomizer(0, 100)}


class MotionSensor(Device):

    def __init__(self, name, edgenode):
        super().__init__("motion", name, edgenode)

    def get_metrics(self):
        return {"motion": boolean_randomizer()}


class DoorSensor(Device):

    def __init__(self, name, edgenode):
        super().__init__("door", name, edgenode)

    def get_metrics(self):
        return {"door_opened": boolean_randomizer()}


class Fan(Device):

    def __init__(self, name, edgenode):
        super().__init__("fan", name, edgenode)

    def get_metrics(self):
        return {"state": randomizer(["ON", "OFF"]), "speed": integer_randomizer(0, 100)}


class Switch(Device):

    def __init__(self, name, edgenode):
        super().__init__("switch", name, edgenode)

    def get_metrics(self):
        return {"state": randomizer(["ON", "OFF"])}


class Speaker(Device):

    def __init__(self, name, edgenode):
        super().__init__("speaker", name, edgenode)

    def get_metrics(self):
        return {"state": randomizer(["ON", "OFF"]), "volume": integer_randomizer(0, 100), "playing": boolean_randomizer()}


class EdgeNode:

    def __init__(self, group, id, server, port):
        self.group = group
        self.id = id
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.handle_message
        self.server = server
        self.port = port
        self.seq = 0
        self.aliasCounter = 2  # bdSeq is 0, Node Control/Rebirth should not have an alias

        self.metrics = {"bdSeq": 0, "Node Control/Rebirth": False}
        self.mapping = {"bdSeq": 0}

        self.devices = []

        self.connect()

    def handle_message(self, client, userdata, msg):
        print("[" + self.id + "] RECV " + msg.topic + " " + str(msg.payload))

    def connect(self):
        self.client.subscribe("spBv1.0/" + self.group + "/NCMD/" + self.id, 1)
        self.client.will_set("spBv1.0/" + self.group +
                             "/NDEATH/" +
                             self.id, generate_metric(
                                 0, self.metrics, self.mapping), 1, False)

        self.client.connect(self.server, self.port, KEEPALIVE)
        self.client.loop_start()

    def on_disconnect(self, client, userdata, rc):
        print("Disconnected with result code " + str(rc))

    def on_connect(self, client, userdata, flags, rc):
        client.publish("spBv1.0/" + self.group + "/NBIRTH/" +
                       self.id, generate_metric(self.seq,
                                self.metrics, self.mapping, True), 0, False)
        self.seq += 1
        self.metrics["bdSeq"] += 1

        for device in self.devices:
            self.register_device(device)

    def register_device(self, device):
        device_metrics = device.get_metrics()

        if device not in self.devices:
            self.devices.append(device)
            # map metrics to aliases
            for name in device_metrics.keys():
                device.mapping[name] = self.aliasCounter
                self.aliasCounter += 1

        # generate birth metrics, which includes name, alias, and datatype
        # subsequent DDATA messages will only include the alias and value

        birth_metrics = generate_metric(
            self.seq, device_metrics, device.mapping, True)

        self.client.message_callback_add(
            "spBv1.0/" + self.group + "/DCMD/" + self.id + "/" + device.name, device.handle_message)

        self.client.publish("spBv1.0/" + self.group + "/DBIRTH/" + self.id + "/" + device.name,
                            birth_metrics, 0, False)
        self.seq += 1

    def publish(self, device, metrics):
        self.client.publish("spBv1.0/" + self.group + "/DDATA/" + self.id + "/" + device.name,
                            generate_metric(self.seq, metrics, device.mapping, True), 0, False)
        self.seq += 1

    def publish_all(self):
        for device in self.devices:
            self.publish(device, device.get_metrics())

def main():

    e = EdgeNode("bedroom", "node1", "localhost", 1883)
    nodes = [Light, TemperatureSensor, HumiditySensor,
             MotionSensor, DoorSensor, Fan, Switch, Speaker]
    while True:
        time.sleep(2)
        node = random.choice(nodes)
        node = node("device" + str(random.randint(0, 10**10)), e)
        e.publish_all()


if __name__ == "__main__":
    main()
