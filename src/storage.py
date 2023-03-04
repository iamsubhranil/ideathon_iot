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

CONNECTION: sqlite3.Connection = None
WRITELOCK: Lock = Lock()


def serialized(func):
    def wrapper(*args, **kwargs):
        with WRITELOCK:
            return func(*args, **kwargs)
    return wrapper


def setup(config):
    global CONNECTION
    CONNECTION = sqlite3.connect(config["db"]["url"], check_same_thread=False)
    c = CONNECTION.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS Groups (group_id INTEGER PRIMARY KEY AUTOINCREMENT, group_name TEXT NOT NULL, UNIQUE(group_name) ON CONFLICT IGNORE)")
    c.execute("CREATE TABLE IF NOT EXISTS EdgeNode (edge_node_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER REFERENCES Groups, edge_node_name TEXT, status TEXT, birth_timestamp INTEGER, death_timestamp INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS Device (device_id INTEGER PRIMARY KEY AUTOINCREMENT, edge_node_id INTEGER RERFERENCES EdgeNode, device_name TEXT, status TEXT, birth_timestamp INTEGER, death_timestamp INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS Metric (metric_id INTEGER PRIMARY KEY AUTOINCREMENT, device_id INTEGER RERFERENCES Device, metric_name TEXT, metric_type TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS MetricString (metric_id INTEGER REFERENCES Metric, metric_value TEXT NOT NULL, timestamp INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS MetricInt (metric_id INTEGER REFERENCES Metric, metric_value INTEGER NOT NULL, timestamp INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS MetricFloat (metric_id INTEGER RERFERENCES Metric, metric_value REAL NOT NULL, timestamp INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS MetricBoolean (metric_id INTEGER REFERENCES Metric, metric_value INTEGER NOT NULL, timestamp INTEGER)")


def execute_query(query, args=()):
    c = CONNECTION.cursor()
    # print("QUERY: " + query + " ARGS: " + str(args))
    ret = c.execute(query, args).fetchall()
    return ret


@serialized
def insert_group(group_name):
    return execute_query("INSERT OR IGNORE INTO Groups (group_name) VALUES (?)", (group_name,))


@serialized
def insert_edge_node(group_name, edge_node_name, status, birth_timestamp, death_timestamp):
    return execute_query("INSERT OR IGNORE INTO EdgeNode (group_id, edge_node_name, status, birth_timestamp, death_timestamp) VALUES ((SELECT group_id FROM Groups WHERE group_name = ?), ?, ?, ?, ?)", (group_name, edge_node_name, status, birth_timestamp, death_timestamp))


@serialized
def insert_device(group_name, edge_node_name, device_name, status, birth_timestamp, death_timestamp):
    return execute_query("INSERT OR IGNORE INTO Device (edge_node_id, device_name, status, birth_timestamp, death_timestamp) VALUES ((SELECT edge_node_id FROM EdgeNode WHERE edge_node_name = ? AND group_id = (SELECT group_id FROM Groups WHERE group_name = ?)), ?, ?, ?, ?)", (edge_node_name, group_name, device_name, status, birth_timestamp, death_timestamp))


@serialized
def insert_metric(group_name, edge_node_name, device_name, metric_name, metric_type):
    return execute_query("INSERT OR IGNORE INTO Metric (device_id, metric_name, metric_type) VALUES ((SELECT device_id FROM Device WHERE device_name = ? AND edge_node_id = (SELECT edge_node_id FROM EdgeNode WHERE edge_node_name = ? AND group_id = (SELECT group_id FROM Groups WHERE group_name = ?))), ?, ?)", (device_name, edge_node_name, group_name, metric_name, metric_type))


@serialized
def insert_metric_value(metric_id, metric_type, metric_value, timestamp):
    name = metric_type.capitalize()
    return execute_query("INSERT INTO Metric{} (metric_id, metric_value, timestamp) VALUES (?, ?, ?)".format(name), (metric_id, metric_value, timestamp))


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
    return execute_query("SELECT metric_value, timestamp FROM {} WHERE metric_id = ?".format(table_name), (get_metric_id(group_name, edge_node_name, device_name, metric_name),))[0][0]


@serialized
def update_edge_node_status(group_name, edge_node_name, status, timestamp):
    return execute_query("UPDATE EdgeNode SET status = ?, death_timestamp = ? WHERE edge_node_name = ? AND group_id = ?", (status, timestamp, edge_node_name, get_group_id(group_name)))


@serialized
def update_device_status(group_name, edge_node_name, device_name, status, timestamp):
    return execute_query("UPDATE Device SET status = ?, death_timestamp = ? WHERE device_name = ? AND edge_node_id = ?", (status, timestamp, device_name, get_edge_node_id(edge_node_name, group_name)))


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


def get_device_by_name(group_name, edge_node_name, device_name):
    query = "SELECT * from Device WHERE device_name like '%" + device_name + "%' "
    if edge_node_name:
        query += "AND edge_node_id in (SELECT edge_node_id FROM EdgeNode WHERE edge_node_name like '%" + \
            edge_node_name + "%' "
        if group_name:
            query += "AND group_id in (SELECT group_id FROM Groups WHERE group_name like '%" + \
                group_name + "%')"
        query += ")"
    return execute_query(query)


def get_metrics_by_device(device_id):
    metrics = execute_query(
        "SELECT * FROM Metric WHERE device_id = ?", (device_id,))
    ret = []
    for metric in metrics:
        metric_type = metric[3]
        table_name = "Metric" + metric_type.capitalize()
        metric_value = execute_query(
            "SELECT metric_value, timestamp FROM {} WHERE metric_id = ? ORDER BY timestamp DESC".format(table_name), (metric[0],))[0]
        ret.append((metric[2], metric[3], metric_value[0], metric_value[1]))

    return ret
