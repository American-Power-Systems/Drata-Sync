import os
import json
import csv
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

DRATA_API_KEY = os.getenv("DRATA_API_KEY", "").strip()
DRATA_BASE_URL = os.getenv("DRATA_BASE_URL", "https://api.drata.com").strip()
DRATA_WORKSPACE_ID = os.getenv("DRATA_WORKSPACE_ID", "").strip()
DRATA_DATASOURCE_ID = os.getenv("DRATA_DATASOURCE_ID", "").strip()

CSV_PATH = os.getenv("TRAINING_CSV_PATH", "training_completions.csv").strip()

APP_AUTH_TOKEN = os.getenv("APP_AUTH_TOKEN", "").strip()


def require_auth():
    if not APP_AUTH_TOKEN:
        return
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {APP_AUTH_TOKEN}":
        return jsonify({"error": "Unauthorized"}), 401


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    email = (rec.get("employee_email") or rec.get("email") or "").strip().lower()
    if not email:
        raise ValueError("Missing employee email")

    training_name = (rec.get("training_name") or rec.get("course") or "Security Training").strip()

    completed_at = rec.get("completed_at") or rec.get("completion_date") or rec.get("completedAt")
    if not completed_at:
        raise ValueError(f"Missing completed_at for {email}")

    completed_iso = parse_date_to_iso(completed_at)

    proof_url = (rec.get("proof_url") or rec.get("certificate_url") or rec.get("proofUrl") or "").strip()
    proof_text = (rec.get("proof_text") or rec.get("proofText") or "").strip()

    status = (rec.get("status") or "completed").strip().lower()

    return {
        "employee_email": email,
        "training_name": training_name,
        "completed_at": completed_iso,
        "status": status,
        "proof_url": proof_url,
        "proof_text": proof_text,
        "source": rec.get("source", "replit-sync"),
        "synced_at": utc_now_iso(),
    }


def parse_date_to_iso(value: str) -> str:
    s = str(value).strip()
    try:
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass

    fmts = [
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            continue

    raise ValueError(f"Unrecognized date format: {value}")


def load_records_from_csv(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []

    out: List[Dict[str, Any]] = []
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            out.append(row)
    return out


def drata_headers() -> Dict[str, str]:
    if not DRATA_API_KEY:
        raise RuntimeError("Missing DRATA_API_KEY (set it in Replit Secrets).")
    return {
        "Authorization": f"Bearer {DRATA_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def push_to_drata_custom_connection(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not DRATA_WORKSPACE_ID or not DRATA_DATASOURCE_ID:
        return {
            "ok": False,
            "error": "Missing DRATA_WORKSPACE_ID or DRATA_DATASOURCE_ID. Set these in Replit Secrets once you know them.",
            "sent": 0,
        }

    url = f"{DRATA_BASE_URL}/v1/workspaces/{DRATA_WORKSPACE_ID}/custom-connections/{DRATA_DATASOURCE_ID}/records"

    payload = {"records": records}

    r = requests.post(url, headers=drata_headers(), data=json.dumps(payload), timeout=60)
    if r.status_code >= 300:
        return {
            "ok": False,
            "status_code": r.status_code,
            "response_text": r.text[:2000],
            "sent": len(records),
            "url_used": url,
        }

    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}

    return {"ok": True, "sent": len(records), "drata_response": body, "url_used": url}


@app.get("/health")
def health():
    return jsonify({"ok": True, "time": utc_now_iso()})


@app.post("/training/import")
def training_import():
    maybe = require_auth()
    if maybe:
        return maybe

    data = request.get_json(silent=True) or {}
    raw_records = data.get("records") or []
    if not isinstance(raw_records, list):
        return jsonify({"error": "records must be a list"}), 400

    normalized = []
    errors = []
    for i, rec in enumerate(raw_records):
        try:
            normalized.append(normalize_record(rec))
        except Exception as e:
            errors.append({"index": i, "error": str(e), "record": rec})

    return jsonify({"imported": len(normalized), "errors": errors, "normalized_preview": normalized[:5]})


@app.post("/sync")
def sync():
    maybe = require_auth()
    if maybe:
        return maybe

    data = request.get_json(silent=True) or {}
    inline_records = data.get("records") if isinstance(data.get("records"), list) else []
    use_csv = bool(data.get("use_csv", True))

    raw_records: List[Dict[str, Any]] = []
    if use_csv:
        raw_records.extend(load_records_from_csv(CSV_PATH))
    raw_records.extend(inline_records)

    if not raw_records:
        return jsonify({"ok": False, "error": "No records found. Provide records or upload training_completions.csv."}), 400

    normalized: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for i, rec in enumerate(raw_records):
        try:
            normalized.append(normalize_record(rec))
        except Exception as e:
            errors.append({"index": i, "error": str(e), "record": rec})

    try:
        result = push_to_drata_custom_connection(normalized)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "normalized": len(normalized), "errors": errors}), 500

    return jsonify({
        "ok": result.get("ok", False),
        "normalized": len(normalized),
        "errors": errors,
        "drata": result,
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
