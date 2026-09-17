"""Idempotently provision the development connector after Flyway and Connect are ready."""

import json
import os
import time
import urllib.error
import urllib.request

base = os.environ["CONNECT_URL"]
name = "counter-snapshot-connector"
with open("/config/counter-connector.json", encoding="utf-8") as source:
    config = json.load(source)

request = urllib.request.Request(
    f"{base}/connectors/{name}/config",
    data=json.dumps(config).encode(),
    headers={"Content-Type": "application/json"},
    method="PUT",
)
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        print(f"Registered {name}: HTTP {response.status}", flush=True)
except urllib.error.HTTPError as error:
    raise RuntimeError(
        f"Connector registration failed: HTTP {error.code}: "
        f"{error.read().decode('utf-8', errors='replace')}"
    ) from error

deadline = time.monotonic() + 120
while time.monotonic() < deadline:
    with urllib.request.urlopen(f"{base}/connectors/{name}/status", timeout=10) as response:
        status = json.load(response)
    tasks = status.get("tasks", [])
    if status["connector"]["state"] == "RUNNING" and tasks and all(
        task["state"] == "RUNNING" for task in tasks
    ):
        print(f"{name} and its task are RUNNING", flush=True)
        break
    if any(task["state"] == "FAILED" for task in tasks):
        raise RuntimeError(f"Connector task failed: {status}")
    time.sleep(1)
else:
    raise RuntimeError(f"Connector did not reach RUNNING: {status}")
