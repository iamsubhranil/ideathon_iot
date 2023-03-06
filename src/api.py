import fastapi
import storage

app = fastapi.FastAPI()


def to_dict(rows):
    return [dict(row) for row in rows]


@app.on_event("startup")
async def startup_event():
    storage.setup()
    storage.set_return_dict(True)


@app.on_event("shutdown")
async def shutdown_event():
    storage.shutdown()


# get the list of groups
@app.get("/groups")
async def get_groups() -> list[dict]:
    return to_dict(storage.get_groups())


# get the list of edge nodes
@app.get("/edge_nodes")
async def get_edge_nodes() -> list[dict]:
    return to_dict(storage.get_edge_nodes())


# get the list of devices
@app.get("/devices")
async def get_devices() -> list[dict]:
    return to_dict(storage.get_devices())


# get group details by id
@app.get("/groups/{group_id}")
async def get_group_by_id(group_id: int) -> list[dict]:
    return to_dict(storage.get_group_by_id(group_id))


# get edge node details by id
@app.get("/edge_nodes/{edge_node_id}")
async def get_edge_node_by_id(edge_node_id: int) -> list[dict]:
    return to_dict(storage.get_edge_node_by_id(edge_node_id))


# get device details by id
@app.get("/devices/{device_id}")
async def get_device_by_id(device_id: int) -> list[dict]:
    return to_dict(storage.get_device_by_id(device_id))


# get all devices for a given group
@app.get("/groups/{group_id}/devices")
async def get_devices_by_group(group_id: int) -> list[dict]:
    return to_dict(storage.get_devices_by_group(group_id))


# get all devices for a given edge node
@app.get("/edge_nodes/{edge_node_id}/devices")
async def get_devices_by_edge_node(edge_node_id: int) -> list[dict]:
    return to_dict(storage.get_devices_by_edge_node(edge_node_id))


# get all nodes for a given group
@app.get("/groups/{group_id}/edge_nodes")
async def get_edge_nodes_by_group(group_id: int) -> list[dict]:
    return to_dict(storage.get_edge_nodes_by_group(group_id))


# get all metrics for a given device
@app.get("/devices/{device_id}/metrics")
async def get_metrics_by_device(device_id: int):
    fields = ["name", "type", "value", "timestamp"]
    metrics = storage.get_metrics_by_device(device_id)
    return [{field: metric[i] for i, field in enumerate(fields)} for metric in metrics]
