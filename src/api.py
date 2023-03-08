import fastapi
import model
from typing import Any

# The FastAPI app
app = fastapi.FastAPI()


@app.on_event("startup")
async def startup_event():
    model.startup()


@app.on_event("shutdown")
async def shutdown_event():
    model.shutdown()


# Extract the given attributes from the given objects
# and return a list of dictionaries
def extract(objects: list[Any], attrs: list[str]) -> list[dict[str, Any]]:
    return [{attr: getattr(object, attr) for attr in attrs} for object in objects]


# get the list of groups
@app.get("/groups")
async def get_groups() -> list[dict]:
    return extract(model.get_groups(), ["id", "name"])


# get the list of edge nodes
@app.get("/nodes")
async def get_edge_nodes() -> list[dict]:
    return extract(model.get_nodes(), ["id", "name", "status", "birth_timestamp", "death_timestamp"])


# get the list of devices
@app.get("/devices")
async def get_devices() -> list[dict]:
    return extract(model.get_devices(), ["id", "name", "status", "birth_timestamp", "death_timestamp"])


# get group details by id
@app.get("/groups/{group_id}")
async def get_group_by_id(group_id: int) -> list[dict]:
    return extract([model.Group(group_id)], ["id", "name"])


# get edge node details by id
@app.get("/nodes/{edge_node_id}")
async def get_edge_node_by_id(edge_node_id: int) -> list[dict]:
    return extract([model.Node(edge_node_id)], ["id", "name", "status", "birth_timestamp", "death_timestamp"])


# get device details by id
@app.get("/devices/{device_id}")
async def get_device_by_id(device_id: int) -> list[dict]:
    return extract([model.Device(device_id)], ["id", "name", "status", "birth_timestamp", "death_timestamp"])


# get all devices for a given group
@app.get("/groups/{group_id}/devices")
async def get_devices_by_group(group_id: int) -> list[dict]:
    return extract(model.Group(group_id).devices, ["id", "name", "status", "birth_timestamp", "death_timestamp"])


# get all devices for a given edge node
@app.get("/nodes/{edge_node_id}/devices")
async def get_devices_by_edge_node(edge_node_id: int) -> list[dict]:
    return extract(model.Node(edge_node_id).devices, ["id", "name", "status", "birth_timestamp", "death_timestamp"])


# get all nodes for a given group
@app.get("/groups/{group_id}/nodes")
async def get_edge_nodes_by_group(group_id: int) -> list[dict]:
    return extract(model.Group(group_id).nodes, ["id", "name", "status", "birth_timestamp", "death_timestamp"])


# get all metrics for a given device
@app.get("/devices/{device_id}/metrics")
async def get_metrics_by_device(device_id: int):
    return extract(model.Device(device_id).metrics, ["id", "name", "type", "value", "timestamp"])
