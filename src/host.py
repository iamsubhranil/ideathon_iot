import paho.mqtt.client as mqtt
import sparkplug_b_pb2 as payload
import json
import time
import storage
import repl


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


def generate_metric(seq, metrics, mapping, birth=False, timestamp=int(time.time())):
    payload1 = payload.Payload()
    payload1.timestamp = timestamp
    payload1.seq = seq

    for name, value in metrics.items():
        metric(payload1, None if mapping ==
               None or name not in mapping else mapping[name], value, name, birth)

    return payload1.SerializeToString()


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
        print(metric.datatype)


class SparkplugHost:

    def __init__(self, config):
        self.config = config
        self.client = mqtt.Client()
        self.ts = int(time.time())
        self.client.will_set("spBv1.0/STATE/" + self.config["id"],
                             json.dumps({"online": False, "timestamp": self.ts}), qos=1, retain=True)
        self.client.connect(self.config["mqtt"]
                            ["host"], self.config["mqtt"]["port"])
        self.client.subscribe("spBv1.0/STATE/" + self.config["id"])

        self.edgeNodeSeq = {}  # maps the sequence number to each edge node
        self.edgeNodeAlive = {}  # maps the alive status to each edge node
        # register handlers
        self.client.loop_start()
        self.client.publish("spBv1.0/STATE/" + self.config["id"],
                            payload=json.dumps({"online": True, "timestamp": self.ts}), qos=1, retain=True)
        for group in config["zones"]:
            self.client.subscribe("spBv1.0/" + group + "/NBIRTH/#")
            self.client.message_callback_add(
                "spBv1.0/" + group + "/NBIRTH/#", self.handle_action)
            self.client.subscribe("spBv1.0/" + group + "/DBIRTH/#")
            self.client.message_callback_add(
                "spBv1.0/" + group + "/DBIRTH/#", self.handle_action)
            self.client.subscribe("spBv1.0/" + group + "/NDATA/#")
            self.client.message_callback_add(
                "spBv1.0/" + group + "/NDATA/#", self.handle_action)
            self.client.subscribe("spBv1.0/" + group + "/DDATA/#")
            self.client.message_callback_add(
                "spBv1.0/" + group + "/DDATA/#", self.handle_action)
            self.client.subscribe("spBv1.0/" + group + "/NDEATH/#")
            self.client.message_callback_add(
                "spBv1.0/" + group + "/NDEATH/#", self.handle_action)
            self.client.subscribe("spBv1.0/" + group + "/DDEATH/#")
            self.client.message_callback_add(
                "spBv1.0/" + group + "/DDEATH/#", self.handle_action)

        # start network traffic and publish birth certificate
        self.client.loop_start()
        self.client.publish("spBv1.0/STATE/" + self.config["id"],
                            payload=json.dumps({"online": True, "timestamp": self.ts}), qos=1, retain=True)

    def extract_payload(self, msg):
        p = payload.Payload()
        p.ParseFromString(msg.payload)
        return p

    def extract_msg(self, msg):
        parts = msg.topic.split("/")
        group_name = parts[1]
        action = parts[2]
        node_name = parts[3]
        device_name = None
        if len(parts) == 5:
            device_name = parts[4]

        return group_name, node_name, device_name, action, self.extract_payload(msg)

    def send_rebirth(self, group_name, node_name):
        self.client.publish("spBv1.0/" + group_name + "/NCMD/" + node_name,
                            payload=generate_metric(0, {"Node Control/Rebirth": True}, None, True),)
        self.edgeNodeAlive[group_name + node_name] = True

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

    def handle_nbirth(self, group_name, node_name, payload):
        print("[NEW] Node discovered " + node_name)
        self.edgeNodeSeq[group_name + node_name] = payload.seq
        self.edgeNodeAlive[group_name + node_name] = True
        storage.insert_group(group_name)
        storage.insert_edge_node(
            group_name, node_name, "ONLINE", payload.timestamp, 0)

    def handle_dbirth(self, group_name, node_name, device_name, payload):
        print("[NEW] Device discovered " + node_name + "/" + device_name)
        storage.insert_device(group_name, node_name,
                              device_name, "ONLINE", payload.timestamp, 0)
        if len(payload.metrics) == 0:
            raise Exception("No metrics in device birth certificate")
        for metric in payload.metrics:
            metric_type_str, value = get_metric_type_string(metric)
            storage.insert_metric(group_name, node_name,
                                  device_name, metric.name, metric_type_str)
            metric_id = storage.get_metric_id(
                group_name, node_name, device_name, metric.name)
            storage.insert_metric_value(
                metric_id, metric_type_str, value, payload.timestamp)

    def handle_ndata(self, group_name, node_name, payload):
        print("[NEW] Node discovered " + node_name)
        self.edgeNodeSeq[group_name + node_name] += 1

    def handle_ddata(self, group_name, node_name, device_name, payload):
        # print("DDATA: " + group_name + "/" + node_name + "/" + device_name)
        for metric in payload.metrics:
            metric_type_str, value = get_metric_type_string(metric)
            metric_id = storage.get_metric_id(
                group_name, node_name, device_name, metric.name)
            storage.insert_metric_value(
                metric_id, metric_type_str, value, payload.timestamp)

    def handle_ndeath(self, group_name, node_name, payload):
        # print("NDEATH: " + node_name)
        self.edgeNodeAlive[group_name + node_name] = False
        storage.update_edge_node_status(
            group_name, node_name, "OFFLINE", payload.timestamp)

    def handle_ddeath(self, group_name, node_name, device_name, payload):
        # print("DDEATH: " + node_name + "/" + device_name)
        storage.update_device_status(
            group_name, node_name, device_name, "OFFLINE", payload.timestamp)


def load_config():
    with open("config.json", "rb") as f:
        return json.load(f)


def main():
    config = load_config()
    storage.setup(config)
    host = SparkplugHost(config)

    while True:
        repl.SparkplugREPL().cmdloop()

    # host is now online, register the appropriate handlers


if __name__ == "__main__":
    main()