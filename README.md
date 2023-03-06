# A Sparkplug B simulator and host system.

This project simulates a bunch of Sparkplug B client devices and a collection
of softwares that can gather and process the data.

To run the simulator, you need to have a working installation of Python 3.6 or
higher. You can install the required packages with the following command:

```
$ pip install -r requirements.txt
```

First, start the host system:

```
$ python host.py
```

Then, start the simulator:

```
$ python client.py
```

The simulator will start sending data to the host system. The host system will
process the data and store it in a database. You can then interface with the data
using either the CLI or the REST API.

To use the CLI, run the following command:

```
$ python repl.py
=> help
```

To use the REST API, run the following command:

```
$ uvicorn api:app --reload
```

You can then access the API at `http://localhost:8000`. The API documentation
is available at `http://localhost:8000/docs`.
