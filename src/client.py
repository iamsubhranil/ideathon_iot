import paho.mqtt.client as mqtt
import time
import random
import json
import string
import datetime

import sparkplug_b_pb2 as payload

KEEPALIVE = 60


# Generate a metric for a payload
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


# Generate a payload from raw metrics
def generate_payload(seq, metrics, mapping, birth=False, timestamp=None):
    payload1 = payload.Payload()
    if timestamp is None:
        timestamp = int(time.time())
    payload1.timestamp = timestamp
    payload1.seq = seq

    for name, value in metrics.items():
        metric(
            payload1, None if name not in mapping else mapping[name], value, name, birth)

    return payload1.SerializeToString()


# Randomly generate a string of length 10
def string_randomizer():
    return "".join(random.choices(string.ascii_letters, k=10))


# Randomly generate an integer between 0 and 100
def int_randomizer(min=0, max=100):
    return random.randint(min, max)


# Randomly generate a float between 0 and 100
def float_randomizer(min=0.0, max=100.0):
    return random.uniform(min, max)


# Randomly generate a boolean
def boolean_randomizer():
    return random.choice([True, False])


# Randomly generate a value based on the type
def randomize(value_type):
    if value_type == "string":
        return string_randomizer()
    elif value_type == "int":
        return int_randomizer()
    elif value_type == "float":
        return float_randomizer()
    elif value_type == "boolean":
        return boolean_randomizer()


# Base class for a device
class Device:

    # name: name of the device
    # metrics: dictionary of metric names and types
    def __init__(self, name, metrics):
        self.type = type
        self.name = name
        self.metrics = metrics
        self.mapping = {}

    # Called by the edge node to get the current values of the metrics
    def get_metrics(self):
        values = {name: randomize(value_type)
                  for name, value_type in self.metrics.items()}
        return values

    def handle_message(self, client, userdata, msg):
        print("[" + self.name + "] RECV " + msg.topic + " " + str(msg.payload))


# Base class for an edge node
class EdgeNode:

    # group: group name
    # id: edge node id
    # server: MQTT server address
    # port: MQTT server port
    def __init__(self, group, id, server, port):
        self.group = group
        self.id = id
        self.client = mqtt.Client()
        # register callbacks
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.handle_message
        self.server = server
        self.port = port
        self.seq = 0
        self.aliasCounter = 2  # bdSeq is 0, Node Control/Rebirth should not have an alias

        # register base metrics for the node
        self.metrics = {"bdSeq": 0, "Node Control/Rebirth": False}
        self.mapping = {"bdSeq": 0}

        # list of devices
        self.devices = []

        self.init_done = False
        self.connect()

    # Called when a message is received
    def handle_message(self, client, userdata, msg):
        print("[" + self.id + "] RECV " + msg.topic + " " + str(msg.payload))

    # Called to establish a connection to the MQTT server
    def connect(self):
        self.client.subscribe("spBv1.0/" + self.group + "/NCMD/" + self.id, 1)
        self.client.will_set("spBv1.0/" + self.group +
                             "/NDEATH/" +
                             self.id, generate_payload(
                                 0, self.metrics, self.mapping), 1, False)

        self.client.connect(self.server, self.port, KEEPALIVE)
        self.client.loop_start()

    # Called when the edge node is disconnected from the MQTT server
    def on_disconnect(self, client, userdata, rc):
        print("Disconnected with result code " + str(rc))

    # Called when the edge node is connected to the MQTT server
    def on_connect(self, client, userdata, flags, rc):
        # publish birth certificate
        client.publish("spBv1.0/" + self.group + "/NBIRTH/" +
                       self.id, generate_payload(self.seq,
                                                 self.metrics, self.mapping, True), 0, False)
        self.seq += 1
        self.metrics["bdSeq"] += 1

        # register devices
        for device in self.devices:
            self.register_device(device)

        self.init_done = True

    # Called to register a device
    def register_device(self, device):
        # get metrics from device
        device_metrics = device.get_metrics()

        # add device to list of devices
        if device not in self.devices:
            self.devices.append(device)
            # map metrics to aliases
            for name in device_metrics.keys():
                device.mapping[name] = self.aliasCounter
                self.aliasCounter += 1

        # generate birth metrics, which includes name, alias, and datatype
        # subsequent DDATA messages will only include the alias and value

        birth_metrics = generate_payload(
            self.seq, device_metrics, device.mapping, True)

        self.client.message_callback_add(
            "spBv1.0/" + self.group + "/DCMD/" + self.id + "/" + device.name, device.handle_message)

        self.client.publish("spBv1.0/" + self.group + "/DBIRTH/" + self.id + "/" + device.name,
                            birth_metrics, 0, False)
        self.seq += 1

    # Called to publish metrics for a device
    def publish(self, device, metrics):
        self.client.publish("spBv1.0/" + self.group + "/DDATA/" + self.id + "/" + device.name,
                            generate_payload(self.seq, metrics, device.mapping, True), 0, False)
        self.seq += 1

    # Called to publish metrics for all devices
    def publish_all(self):
        for device in self.devices:
            self.publish(device, device.get_metrics())


def main():
    print("Loading config..")
    # load config
    config = json.load(open("config.json"))

    zones = config["zones"]
    nodes = []

    # get number of nodes and devices per node
    node_count = config["client_node_count"]
    device_count = config["client_devices_per_node"]
    device_types = list(config["client_device_types"].keys())

    print("Creating nodes..")
    # create nodes
    for i in range(node_count):
        print("[Node " + str(i) + "] Creating node..")
        # pick a random MQTT server and zone
        mqtt_server = random.choice(config["mqtt"])
        zone = random.choice(zones)
        # create node
        e = EdgeNode(zone, "node" + str(i),
                     mqtt_server["host"], mqtt_server["port"])
        nodes.append(e)

        print("[Node " + str(i) + "] Waiting for node to connect..")
        # wait for node to connect
        while not e.init_done:
            time.sleep(0.1)

        print("[Node " + str(i) + "] Registering devices..")
        # register devices
        for j in range(device_count):
            # pick a random device type
            device = random.choice(device_types)
            metrics = config["client_device_types"][device]["metrics"]
            metric_types = {
                metric: config["client_metric_types"][metric] for metric in metrics}
            # create device
            device = Device(device + str(j), metric_types)
            # register device
            e.register_device(device)

    print("Starting publish metrics..")
    # publish metrics
    while True:
        print("[" + datetime.datetime.now().strftime("%H:%M:%S") +
              "] Publishing metrics..")
        # publish metrics for all devices
        for node in nodes:
            node.publish_all()
        # wait for interval
        time.sleep(config["client_publish_interval_seconds"])


if __name__ == "__main__":
    main()
