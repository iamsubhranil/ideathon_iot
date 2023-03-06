import paho.mqtt.client as mqtt
import sparkplug_b_pb2 as payload
import json
import time
import model


# Generate a metric for the payload
# alias is the alias of the metric
# value is the value of the metric
# name is the name of the metric
# birth is a boolean to indicate if the metric is a birth metric
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


# Generate a payload for the MQTT message
# metrics is a list of name:value
def generate_metric(seq, metrics, mapping, birth=False, timestamp=int(time.time())):
    payload1 = payload.Payload()
    payload1.timestamp = timestamp
    payload1.seq = seq

    for name, value in metrics.items():
        metric(payload1, None if mapping ==
               None or name not in mapping else mapping[name], value, name, birth)

    return payload1.SerializeToString()


# Convert a Sparkplug metric type to a string
# and return the value
def get_metric_type_string(metric):
    if metric.datatype == payload.Int64:
        return "int", metric.int_value
    elif metric.datatype == payload.Float:
        return "float", metric.float_value
    elif metric.datatype == payload.Boolean:
        return "boolean", metric.boolean_value
    elif metric.datatype == payload.String:
        return "string", metric.string_value
    else:
        raise Exception("Unknown metric type")


# A configurable MQTT client that handles the Sparkplug B protocol
class SparkplugHost:

    # Initialize the MQTT client
    def __init__(self, config):
        self.config = config
        self.client = mqtt.Client()
        self.ts = int(time.time())
        print("Setting WILL message..")
        self.client.will_set("spBv1.0/STATE/" + self.config["id"],
                             json.dumps({"online": False, "timestamp": self.ts}), qos=1, retain=True)
        print("Connecting to MQTT broker..")
        self.client.connect(self.config["mqtt"]
                            ["host"], self.config["mqtt"]["port"])
        self.client.subscribe("spBv1.0/STATE/" + self.config["id"])

        self.edgeNodeSeq = {}  # maps the sequence number to each edge node
        self.edgeNodeAlive = {}  # maps the alive status to each edge node
        # register handlers for all the actions in all of the zones
        print("Registering handlers..")
        event_types = ["NBIRTH", "DBIRTH",
                       "NDEATH", "DDEATH", "NDATA", "DDATA"]
        for group in config["zones"]:
            for event_type in event_types:
                self.client.subscribe("spBv1.0/" + group + "/" +
                                      event_type + "/#")
                self.client.message_callback_add(
                    "spBv1.0/" + group + "/" + event_type + "/#", self.handle_action)

    def connect(self):
        # start network traffic and publish birth certificate
        self.client.publish("spBv1.0/STATE/" + self.config["id"],
                            payload=json.dumps({"online": True, "timestamp": self.ts}), qos=1, retain=True)
        self.client.loop_forever()

    # Extract the payload from the MQTT message
    def extract_payload(self, msg):
        p = payload.Payload()
        p.ParseFromString(msg.payload)
        return p

    # Extract the group name, node name, device name, action and payload from the MQTT message
    def extract_msg(self, msg):
        parts = msg.topic.split("/")
        group_name = parts[1]
        action = parts[2]
        node_name = parts[3]
        device_name = None
        if len(parts) == 5:
            device_name = parts[4]

        return group_name, node_name, device_name, action, self.extract_payload(msg)

    # Send a rebirth command to an edge node
    def send_rebirth(self, group_name, node_name):
        self.client.publish("spBv1.0/" + group_name + "/NCMD/" + node_name,
                            payload=generate_metric(0, {"Node Control/Rebirth": True}, None, True),)
        self.edgeNodeAlive[group_name + node_name] = True

    # Extract an incoming message and call the appropriate handler
    def handle_action(self, client, userdata, msg):
        group_name, node_name, device_name, action, payload = self.extract_msg(
            msg)
        if action == "NBIRTH":
            self.handle_nbirth(group_name, node_name, payload)
        elif action == "DBIRTH":
            self.handle_dbirth(group_name, node_name, device_name, payload)
        elif action == "NDATA":
            if not self.edgeNodeAlive[group_name + node_name]:
                self.send_rebirth(group_name, node_name)
            else:
                self.handle_ndata(group_name, node_name, payload)
        elif action == "DDATA":
            self.handle_ddata(group_name, node_name, device_name, payload)
        elif action == "NDEATH":
            self.handle_ndeath(group_name, node_name, payload)
        elif action == "DDEATH":
            self.handle_ddeath(group_name, node_name, device_name, payload)

    # Handle a node birth message
    def handle_nbirth(self, group_name, node_name, payload):
        print("[NEW] Node discovered " + node_name)
        # set the sequence number for this node
        self.edgeNodeSeq[group_name + node_name] = payload.seq
        self.edgeNodeAlive[group_name + node_name] = True
        # create the node in the model
        model.create_group(group_name)
        node = model.create_node(group_name, node_name)
        # set the node status
        node.status = "ONLINE"
        node.birth_timestamp = payload.timestamp
        node.death_timestamp = 0

    # Handle a device birth message
    def handle_dbirth(self, group_name, node_name, device_name, payload):
        print("[NEW] Device discovered " + node_name + "/" + device_name)
        # create the device in the model
        device = model.create_device(group_name, node_name, device_name)
        # set the device status
        device.status = "ONLINE"
        device.birth_timestamp = payload.timestamp
        device.death_timestamp = 0
        # register the device metrics
        if len(payload.metrics) == 0:
            raise Exception("No metrics in device birth certificate")
        for metric in payload.metrics:
            metric_type_str, value = get_metric_type_string(metric)
            # create the metric in the model
            metric = model.create_metric(group_name, node_name,
                                         device_name, metric.name, metric_type_str)
            metric.value = (value, payload.timestamp)

    # Handle a node data message
    def handle_ndata(self, group_name, node_name, payload):
        print("[NEW] Node discovered " + node_name)
        self.edgeNodeSeq[group_name + node_name] += 1
        # currently ignored

    # Handle a device data message
    def handle_ddata(self, group_name, node_name, device_name, payload):
        # print("DDATA: " + group_name + "/" + node_name + "/" + device_name)
        # update the device metrics
        for metric in payload.metrics:
            for metric_i in model.get_device(group_name, node_name, device_name)[0].metrics:
                if metric_i.name == metric.name:
                    _, value = get_metric_type_string(metric)
                    metric_i.value = (value, payload.timestamp)

    # Handle a node death message
    def handle_ndeath(self, group_name, node_name, payload):
        # print("NDEATH: " + node_name)
        # set the node status
        self.edgeNodeAlive[group_name + node_name] = False
        node = model.get_node(group_name, node_name)[0]
        node.status = "OFFLINE"
        node.death_timestamp = payload.timestamp

    # Handle a device death message
    def handle_ddeath(self, group_name, node_name, device_name, payload):
        # print("DDEATH: " + node_name + "/" + device_name)
        # set the device status
        device = model.get_device(group_name, node_name, device_name)[0]
        device.status = "OFFLINE"
        device.death_timestamp = payload.timestamp


# Load the config file
def load_config():
    with open("config.json", "rb") as f:
        return json.load(f)


def main():
    print("Loading config..")
    config = load_config()
    print("Setting up model..")
    model.startup()
    print("Starting host..")
    host = SparkplugHost(config)

    try:
        print("Starting processing loop..")
        host.connect()
    except KeyboardInterrupt:
        model.shutdown()
        print("Shutting down..")
    except:
        model.shutdown()
        print("[Error] Error occurred:")
        raise


if __name__ == "__main__":
    main()
