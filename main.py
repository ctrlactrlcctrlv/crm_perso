import json
import os
from typing import Dict, List, Optional

import requests


def load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def parse_line(line: str) -> Dict[str, Optional[str]]:
    # Expected shape: prenom;nom;fonction;linkedin:entreprise
    # The last field allows "linkedin:entreprise".
    parts = [p.strip() for p in line.split(";")]
    if len(parts) < 4:
        raise ValueError(f"Invalid line (expected 4 fields): {line}")
    prenom, nom, fonction = parts[0], parts[1], parts[2]
    linkedin, entreprise = None, None
    if ":" in parts[3]:
        linkedin, entreprise = [p.strip() or None for p in parts[3].split(":", 1)]
    else:
        linkedin = parts[3] or None
    return {
        "prenom": prenom or None,
        "nom": nom or None,
        "fonction": fonction or None,
        "linkedin": linkedin,
        "entreprise": entreprise,
    }


def load_records(path: str) -> List[Dict[str, Optional[str]]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        records: List[Dict[str, Optional[str]]] = []
        for item in data:
            if isinstance(item, str):
                records.append(parse_line(item))
            elif isinstance(item, dict):
                records.append(
                    {
                        "prenom": item.get("prenom"),
                        "nom": item.get("nom"),
                        "fonction": item.get("fonction"),
                        "linkedin": item.get("linkedin"),
                        "entreprise": item.get("entreprise"),
                    }
                )
            else:
                raise ValueError("Unsupported item in JSON list.")
        return records
    if isinstance(data, dict) and "records" in data and isinstance(data["records"], list):
        return load_records_from_list(data["records"])
    raise ValueError("JSON must be a list of strings or list of objects.")


def load_records_from_list(items: List[object]) -> List[Dict[str, Optional[str]]]:
    records: List[Dict[str, Optional[str]]] = []
    for item in items:
        if isinstance(item, str):
            records.append(parse_line(item))
        elif isinstance(item, dict):
            records.append(
                {
                    "prenom": item.get("prenom"),
                    "nom": item.get("nom"),
                    "fonction": item.get("fonction"),
                    "linkedin": item.get("linkedin"),
                    "entreprise": item.get("entreprise"),
                }
            )
        else:
            raise ValueError("Unsupported item in JSON list.")
    return records


def post_row(api_url: str, token: str, table_id: str, row: Dict[str, Optional[str]]) -> None:
    url = f"{api_url.rstrip('/')}/api/database/rows/table/{table_id}/?user_field_names=true"
    headers = {"Authorization": f"Token {token}"}
    payload = {k: v for k, v in row.items() if v is not None}
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(f"Failed to insert row: {resp.status_code} {resp.text}")


def row_exists_by_linkedin(
    api_url: str, token: str, table_id: str, linkedin: str
) -> bool:
    url = (
        f"{api_url.rstrip('/')}/api/database/rows/table/{table_id}/"
        f"?user_field_names=true&filter__linkedin__equal={linkedin}"
    )
    headers = {"Authorization": f"Token {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(f"Failed to check row: {resp.status_code} {resp.text}")
    data = resp.json()
    return bool(data.get("count", 0))


def main() -> int:
    load_dotenv(".env")
    api_url = os.environ.get("BASEROW_API_URL", "")
    token = os.environ.get("BASEROW_TOKEN", "")
    table_id = os.environ.get("BASEROW_TABLE_ID", "")
    json_path = os.environ.get("CONTACTS_JSON_PATH", "contacts.json")

    if not api_url or not token or not table_id:
        raise SystemExit(
            "Missing config. Set BASEROW_API_URL, BASEROW_TOKEN, BASEROW_TABLE_ID in .env."
        )

    records = load_records(json_path)
    for row in records:
        linkedin = row.get("linkedin")
        if linkedin and row_exists_by_linkedin(api_url, token, table_id, linkedin):
            continue
        post_row(api_url, token, table_id, row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
