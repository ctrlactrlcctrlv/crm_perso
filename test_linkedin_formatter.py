import json
import time
from typing import Any, Dict, List, Optional, Union

import requests

API_KEY = "ef2d40c6-800c-4390-9382-80b0949e09f4"
DATASET_ID = "gd_l1viktl72bvl7bjuj0"


def trigger_snapshot_id(
    profiles: List[Dict[str, str]],
    api_key: str,
    dataset_id: str,
    wait_seconds: int = 10,
    poll_interval_seconds: int = 1,
) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    data = json.dumps({"input": profiles})
    url = (
        "https://api.brightdata.com/datasets/v3/trigger"
        f"?dataset_id={dataset_id}&notify=false&include_errors=true"
    )

    deadline = time.time() + wait_seconds
    while True:
        response = requests.post(url, headers=headers, data=data, timeout=60)
        if response.status_code >= 300:
            raise RuntimeError(
                f"Bright Data trigger error: {response.status_code} {response.text}"
            )
        time.sleep(10)  # brief pause before parsing response
        payload = response.json()
        snapshot_id = payload.get("snapshot_id")
        if snapshot_id:
            return snapshot_id
        if time.time() >= deadline:
            raise TimeoutError(
                "Bright Data trigger did not return a snapshot_id in time."
            )
        time.sleep(poll_interval_seconds)


def get_snapshot_data(
    snapshot_id: str,
    api_key: str,
    timeout: int = 30,
) -> Union[Dict[str, Any], List[Any]]:
    url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code >= 300:
        raise RuntimeError(
            f"Bright Data snapshot error: {response.status_code} {response.text}"
        )
    return response.json()


def wait_for_snapshot_data(
    snapshot_id: str,
    api_key: str,
    wait_seconds: int = 300,
    poll_interval_seconds: int = 30,
) -> Union[Dict[str, Any], List[Any]]:
    deadline = time.time() + wait_seconds
    while True:
        payload = get_snapshot_data(snapshot_id, api_key)
        if isinstance(payload, list):
            if payload:
                return payload
        else:
            status = payload.get("status")
            if status == "ready" or status is None:
                return payload
        if time.time() >= deadline:
            raise TimeoutError(
                "Bright Data snapshot did not become ready in time."
            )
        time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    profiles = [{"url": "https://www.linkedin.com/in/swann-fabre-b57587253/"}]
    snapshot_id = trigger_snapshot_id(profiles, API_KEY, DATASET_ID)
    snapshot_data = wait_for_snapshot_data(snapshot_id, API_KEY)
    print(json.dumps(snapshot_data, indent=2, ensure_ascii=False))
