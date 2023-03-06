"""
DB schema for Sparkplug storage

Group(group_id int autoincr primary, group_name)
EdgeNode(edge_node_id int autoincr primary, group_id refr, edge_node_name, status, birth_timestamp, death_timestamp)
Device(device_id int autoincr primary, edge_node_id refr, device_name, status, birth_timestamp, death_timestamp)
Metric(metric_id int autoincr primary, device_id refr, metric_name, metric_type)

There will be one table for each type of metric (string, int, float, bool)

MetricString(metric_id refr, metric_value, timestamp)
MetricInt(metric_id refr, metric_value, timestamp)
MetricFloat(metric_id refr, metric_value, timestamp)
MetricBool(metric_id refr, metric_value, timestamp)
"""

from threading import Lock
import sqlite3
import json

CONNECTION: sqlite3.Connection = None
WRITELOCK: Lock = Lock()

SETUP_DONE = False


def serialized(func):
    def wrapper(*args, **kwargs):
        with WRITELOCK:
            return func(*args, **kwargs)
    return wrapper


def startup():
    global SETUP_DONE
    if SETUP_DONE:
        return
    config = json.loads(open("config.json", "rb").read())
    global CONNECTION
    CONNECTION = sqlite3.connect(config["db"]["url"], check_same_thread=False)
    c = CONNECTION.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS Groups (group_id INTEGER PRIMARY KEY AUTOINCREMENT, group_name TEXT NOT NULL, UNIQUE(group_name) ON CONFLICT IGNORE)")
    c.execute("CREATE TABLE IF NOT EXISTS EdgeNode (edge_node_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER REFERENCES Groups, edge_node_name TEXT, edge_node_status TEXT, edge_node_birth_timestamp INTEGER, edge_node_death_timestamp INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS Device (device_id INTEGER PRIMARY KEY AUTOINCREMENT, edge_node_id INTEGER RERFERENCES EdgeNode, device_name TEXT, device_status TEXT, device_birth_timestamp INTEGER, device_death_timestamp INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS Metric (metric_id INTEGER PRIMARY KEY AUTOINCREMENT, device_id INTEGER RERFERENCES Device, metric_name TEXT, metric_type TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS MetricString (metric_id INTEGER REFERENCES Metric, metric_value TEXT NOT NULL, metric_timestamp INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS MetricInt (metric_id INTEGER REFERENCES Metric, metric_value INTEGER NOT NULL, metric_timestamp INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS MetricFloat (metric_id INTEGER RERFERENCES Metric, metric_value REAL NOT NULL, metric_timestamp INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS MetricBoolean (metric_id INTEGER REFERENCES Metric, metric_value INTEGER NOT NULL, metric_timestamp INTEGER)")
    SETUP_DONE = True


def shutdown():
    global CONNECTION
    CONNECTION.close()


def set_return_dict(return_dict: bool):
    global CONNECTION
    if return_dict:
        CONNECTION.row_factory = sqlite3.Row
    else:
        CONNECTION.row_factory = None


def execute_query(query, args=()):
    c = CONNECTION.cursor()
    # print("QUERY: " + query + " ARGS: " + str(args))
    ret = c.execute(query, args).fetchall()
    # these calls are serialized, so we can commit here
    if query.startswith("INSERT") or query.startswith("UPDATE") or query.startswith("DELETE"):
        CONNECTION.commit()
    return ret


@serialized
def insert_group(group_name):
    execute_query(
        "INSERT OR IGNORE INTO Groups (group_name) VALUES (?) RETURNING group_id", (group_name,))
    return execute_query("SELECT group_id FROM Groups WHERE group_name = ?", (group_name,))[0][0]


@serialized
def insert_edge_node(group_name, edge_node_name, status, birth_timestamp, death_timestamp):
    execute_query("INSERT OR IGNORE INTO EdgeNode (group_id, edge_node_name, edge_node_status, edge_node_birth_timestamp, edge_node_death_timestamp) VALUES ((SELECT group_id FROM Groups WHERE group_name = ?), ?, ?, ?, ?) RETURNING edge_node_id",
                  (group_name, edge_node_name, status, birth_timestamp, death_timestamp))
    return execute_query("SELECT edge_node_id FROM EdgeNode WHERE edge_node_name = ? AND group_id = (SELECT group_id FROM Groups WHERE group_name = ?)", (edge_node_name, group_name))[0][0]


@serialized
def insert_device(group_name, edge_node_name, device_name, status, birth_timestamp, death_timestamp):
    execute_query("INSERT OR IGNORE INTO Device (edge_node_id, device_name, device_status, device_birth_timestamp, device_death_timestamp) VALUES ((SELECT edge_node_id FROM EdgeNode WHERE edge_node_name = ? AND group_id = (SELECT group_id FROM Groups WHERE group_name = ?)), ?, ?, ?, ?) RETURNING device_id",
                  (edge_node_name, group_name, device_name, status, birth_timestamp, death_timestamp))
    return execute_query("SELECT device_id FROM Device WHERE device_name = ? AND edge_node_id = (SELECT edge_node_id FROM EdgeNode WHERE edge_node_name = ? AND group_id = (SELECT group_id FROM Groups WHERE group_name = ?))", (device_name, edge_node_name, group_name))[0][0]


@serialized
def insert_metric(group_name, edge_node_name, device_name, metric_name, metric_type):
    execute_query("INSERT OR IGNORE INTO Metric (device_id, metric_name, metric_type) VALUES ((SELECT device_id FROM Device WHERE device_name = ? AND edge_node_id = (SELECT edge_node_id FROM EdgeNode WHERE edge_node_name = ? AND group_id = (SELECT group_id FROM Groups WHERE group_name = ?))), ?, ?) RETURNING metric_id",
                  (device_name, edge_node_name, group_name, metric_name, metric_type))
    return execute_query("SELECT metric_id FROM Metric WHERE metric_name = ? AND device_id = (SELECT device_id FROM Device WHERE device_name = ? AND edge_node_id = (SELECT edge_node_id FROM EdgeNode WHERE edge_node_name = ? AND group_id = (SELECT group_id FROM Groups WHERE group_name = ?)))", (metric_name, device_name, edge_node_name, group_name))[0][0]


@serialized
def insert_metric_value(metric_id, metric_type, metric_value, timestamp):
    name = metric_type.capitalize()
    return execute_query("INSERT INTO Metric{} (metric_id, metric_value, metric_timestamp) VALUES (?, ?, ?)".format(name), (metric_id, metric_value, timestamp))


def get_group_id(group_name):
    return execute_query(
        "SELECT group_id FROM Groups WHERE group_name = ?", (group_name,))[0][0]


def get_edge_node_id(group_name, edge_node_name):
    return execute_query("SELECT edge_node_id FROM EdgeNode WHERE edge_node_name = ? AND group_id = ?", (edge_node_name, get_group_id(group_name)))[0][0]


def get_device_id(group_name, edge_node_name, device_name):
    return execute_query("SELECT device_id FROM Device WHERE device_name = ? AND edge_node_id = ?", (device_name, get_edge_node_id(group_name, edge_node_name)))[0][0]


def get_metric_id(group_name, edge_node_name, device_name, metric_name):
    return execute_query("SELECT metric_id FROM Metric WHERE metric_name = ? AND device_id = ?", (metric_name, get_device_id(group_name, edge_node_name, device_name)))[0][0]


def get_metric_type(group_name, edge_node_name, device_name, metric_name):
    return execute_query("SELECT metric_type FROM Metric WHERE metric_name = ? AND device_id = ?", (metric_name, get_device_id(group_name, edge_node_name, device_name)))[0][0]


def get_metric_value(group_name, edge_node_name, device_name, metric_name):
    metric_type = get_metric_type(
        group_name, edge_node_name, device_name, metric_name)
    table_name = "Metric" + metric_type.capitalize()
    return execute_query("SELECT metric_value, metric_timestamp FROM {} WHERE metric_id = ?".format(table_name), (get_metric_id(group_name, edge_node_name, device_name, metric_name),))[0][0]


@serialized
def update_edge_node_status(group_name, edge_node_name, status, timestamp):
    return execute_query("UPDATE EdgeNode SET edge_node_status = ?, edge_node_death_timestamp = ? WHERE edge_node_name = ? AND group_id = ?", (status, timestamp, edge_node_name, get_group_id(group_name)))


@serialized
def update_device_status(group_name, edge_node_name, device_name, status, timestamp):
    return execute_query("UPDATE Device SET device_status = ?, device_death_timestamp = ? WHERE device_name = ? AND edge_node_id = ?", (status, timestamp, device_name, get_edge_node_id(edge_node_name, group_name)))


def get_groups():
    return execute_query("SELECT * FROM Groups")


def get_edge_node_count(group_id):
    return execute_query("SELECT count(*) FROM EdgeNode WHERE group_id = ?", (group_id,))[0][0]


def get_device_count_by_group(group_id):
    return execute_query("SELECT count(*) FROM Device WHERE edge_node_id = (SELECT edge_node_id FROM EdgeNode WHERE group_id = ?)", (group_id,))[0][0]


def get_device_count_by_edge_node(edge_node_id):
    return execute_query("SELECT count(*) FROM Device WHERE edge_node_id = ?", (edge_node_id,))[0][0]


def get_edge_nodes():
    return execute_query("SELECT * FROM EdgeNode")


def get_group_name(group_id):
    return execute_query("SELECT group_name FROM Groups WHERE group_id = ?", (group_id,))[0][0]


def get_group_name_by_edge_node(edge_node_id):
    return execute_query("SELECT group_name FROM Groups WHERE group_id = (SELECT group_id FROM EdgeNode WHERE edge_node_id = ?)", (edge_node_id,))[0][0]


def get_devices():
    return execute_query("SELECT * FROM Device")


def get_edge_node_name(edge_node_id):
    return execute_query("SELECT edge_node_name FROM EdgeNode WHERE edge_node_id = ?", (edge_node_id,))[0][0]


def get_metrics_by_device(device_id):
    metrics = execute_query(
        "SELECT * FROM Metric WHERE device_id = ?", (device_id,))
    ret = []
    for metric in metrics:
        metric_type = metric[3]
        table_name = "Metric" + metric_type.capitalize()
        metric_value = execute_query(
            "SELECT metric_value, metric_timestamp FROM {} WHERE metric_id = ? ORDER BY metric_timestamp DESC".format(table_name), (metric[0],))[0]
        ret.append((metric[2], metric[3], metric_value[0], metric_value[1]))

    return ret


def get_group_by_id(group_id):
    return execute_query("SELECT * FROM Groups WHERE group_id = ?", (group_id,))[0]


def get_edge_node_by_id(edge_node_id):
    return execute_query("SELECT * FROM EdgeNode WHERE edge_node_id = ?", (edge_node_id,))[0]


def get_device_by_id(device_id):
    return execute_query("SELECT * FROM Device WHERE device_id = ?", (device_id,))[0]


def get_devices_by_edge_node(edge_node_id):
    return execute_query("SELECT * FROM Device WHERE edge_node_id = ?", (edge_node_id,))


def get_devices_by_group(group_id):
    return execute_query("SELECT * FROM Device WHERE edge_node_id in (SELECT edge_node_id FROM EdgeNode WHERE group_id = ?)", (group_id,))


def get_edge_nodes_by_group(group_id):
    return execute_query("SELECT * FROM EdgeNode WHERE group_id = ?", (group_id,))


def get_all_groups():
    return execute_query("SELECT group_id FROM Groups")


def get_all_edge_nodes():
    return execute_query("SELECT edge_node_id FROM EdgeNode")


def get_all_devices():
    return execute_query("SELECT device_id FROM Device")


def get_device_by_name(group_name, edge_node_name, device_name):
    query = "SELECT device_id from Device WHERE device_name like '%" + device_name + "%' "
    if edge_node_name:
        query += "AND edge_node_id in (SELECT edge_node_id FROM EdgeNode WHERE edge_node_name like '%" + \
            edge_node_name + "%' "
        if group_name:
            query += "AND group_id in (SELECT group_id FROM Groups WHERE group_name like '%" + \
                group_name + "%')"
        query += ")"
    return execute_query(query)


def get_edge_node_by_name(group_name, edge_node_name):
    query = "SELECT edge_node_id from EdgeNode WHERE edge_node_name like '%" + \
        edge_node_name + "%' "
    if group_name:
        query += "AND group_id in (SELECT group_id FROM Groups WHERE group_name like '%" + \
            group_name + "%')"
    return execute_query(query)


def get_group_by_name(group_name):
    return execute_query("SELECT group_id from Groups WHERE group_name like '%" + group_name + "%'")


def flatten_tuple_list(source_list):
    return [t[0] for t in source_list]


def get(type, id, attr):
    if type == "group":
        if attr == "name":
            return execute_query("SELECT group_name FROM Groups WHERE group_id = ?", (id,))[0][0]
        elif attr == "nodes":
            return flatten_tuple_list(
                execute_query(
                    "SELECT edge_node_id FROM EdgeNode WHERE group_id = ?", (id,)))
        elif attr == "devices":
            return flatten_tuple_list(
                execute_query(
                    "SELECT device_id FROM Device WHERE edge_node_id in " +
                    "(SELECT edge_node_id FROM EdgeNode WHERE group_id = ?)", (id,)))
        else:
            raise ValueError("Invalid attribute for group: " + attr)
    elif type == "node":
        if attr == "name":
            return execute_query("SELECT edge_node_name FROM EdgeNode WHERE edge_node_id = ?", (id,))[0][0]
        elif attr == "devices":
            return flatten_tuple_list(execute_query("SELECT device_id FROM Device WHERE edge_node_id = ?", (id,)))
        elif attr == "group":
            return execute_query("SELECT group_id FROM EdgeNode WHERE edge_node_id = ?", (id,))[0][0]
        elif attr == "status":
            return execute_query("SELECT edge_node_status FROM EdgeNode WHERE edge_node_id = ?", (id,))[0][0]
        elif attr == "birth_timestamp":
            return execute_query("SELECT edge_node_birth_timestamp FROM EdgeNode WHERE edge_node_id = ?", (id,))[0][0]
        elif attr == "death_timestamp":
            return execute_query("SELECT edge_node_death_timestamp FROM EdgeNode WHERE edge_node_id = ?", (id,))[0][0]
        else:
            raise ValueError("Invalid attribute for edge node: " + attr)
    elif type == "device":
        if attr == "name":
            return execute_query("SELECT device_name FROM Device WHERE device_id = ?", (id,))[0][0]
        elif attr == "node":
            return execute_query("SELECT edge_node_id FROM Device WHERE device_id = ?", (id,))[0][0]
        elif attr == "status":
            return execute_query("SELECT device_status FROM Device WHERE device_id = ?", (id,))[0][0]
        elif attr == "metrics":
            return flatten_tuple_list(execute_query(
                "SELECT metric_id FROM Metric WHERE device_id = ?", (id,)))
        elif attr == "group":
            return execute_query("SELECT group_id FROM EdgeNode WHERE edge_node_id = (SELECT edge_node_id FROM Device WHERE device_id = ?)", (id,))[0][0]
        elif attr == "birth_timestamp":
            return execute_query("SELECT device_birth_timestamp FROM Device WHERE device_id = ?", (id,))[0][0]
        elif attr == "death_timestamp":
            return execute_query("SELECT device_death_timestamp FROM Device WHERE device_id = ?", (id,))[0][0]
        else:
            raise ValueError("Invalid attribute for device: " + attr)
    elif type == "metric":
        if attr == "name":
            return execute_query("SELECT metric_name FROM Metric WHERE metric_id = ?", (id,))[0][0]
        elif attr == "type":
            return execute_query("SELECT metric_type FROM Metric WHERE metric_id = ?", (id,))[0][0]
        elif attr == "value" or attr == "values":
            metric_type = execute_query(
                "SELECT metric_type FROM Metric WHERE metric_id = ?", (id,))[0][0]
            table_name = "Metric" + metric_type.capitalize()
            query = "SELECT metric_value FROM {} WHERE metric_id = ?".format(
                table_name)
            if attr == "value":
                query += " ORDER BY metric_timestamp DESC LIMIT 1"
                return execute_query(query, (id,))[0][0]
            else:
                return flatten_tuple_list(execute_query(query, (id,)))
        elif attr == "timestamp":
            metric_type = execute_query(
                "SELECT metric_type FROM Metric WHERE metric_id = ?", (id,))[0][0]
            table_name = "Metric" + metric_type.capitalize()
            return execute_query(
                "SELECT metric_timestamp FROM {} WHERE metric_id = ? ORDER BY metric_timestamp DESC LIMIT 1".format(table_name), (id,))[0][0]
        else:
            raise ValueError("Invalid attribute for type metric: " + attr)
    else:
        raise ValueError("Invalid type")


@serialized
def set(type, id, attr, value):
    if type == "group":
        if attr == "name":
            execute_query(
                "UPDATE Groups SET group_name = ? WHERE group_id = ?", (value, id))
        else:
            raise ValueError("Invalid attribute for group: " + attr)
    elif type == "node":
        if attr == "name":
            execute_query(
                "UPDATE EdgeNode SET edge_node_name = ? WHERE edge_node_id = ?", (value, id))
        elif attr == "status":
            execute_query(
                "UPDATE EdgeNode SET edge_node_status = ? WHERE edge_node_id = ?", (value, id))
        elif attr == "death_timestamp":
            execute_query(
                "UPDATE EdgeNode SET edge_node_death_timestamp = ? WHERE edge_node_id = ?", (value, id))
        elif attr == "birth_timestamp":
            execute_query(
                "UPDATE EdgeNode SET edge_node_birth_timestamp = ? WHERE edge_node_id = ?", (value, id))
        else:
            raise ValueError("Invalid write attribute for edge node: " + attr)
    elif type == "device":
        if attr == "name":
            execute_query(
                "UPDATE Device SET device_name = ? WHERE device_id = ?", (value, id))
        elif attr == "status":
            execute_query(
                "UPDATE Device SET device_status = ? WHERE device_id = ?", (value, id))
        elif attr == "death_timestamp":
            execute_query(
                "UPDATE Device SET device_death_timestamp = ? WHERE device_id = ?", (value, id))
        elif attr == "birth_timestamp":
            execute_query(
                "UPDATE Device SET device_birth_timestamp = ? WHERE device_id = ?", (value, id))
        else:
            raise ValueError("Invalid attribute for device: " + attr)
    elif type == "metric":
        if attr == "value":
            metric_type = execute_query(
                "SELECT metric_type FROM Metric WHERE metric_id = ?", (id,))[0][0]
            table_name = "Metric" + metric_type.capitalize()
            execute_query(
                "INSERT INTO {} (metric_id, metric_value, metric_timestamp) VALUES (?, ?, ?)".format(
                    table_name),
                (id, value[0], value[1]))
        else:
            raise ValueError("Invalid write attribute for metric: " + attr)
    else:
        raise ValueError("Invalid type")
