import os
import json
import csv
from datetime import datetime, timezone
from typing import List, Dict, Any

import psycopg2
import psycopg2.extras
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

DRATA_API_KEY = os.getenv("DRATA_API_KEY", "").strip()
DRATA_BASE_URL = os.getenv("DRATA_BASE_URL", "https://api.drata.com").strip()
DRATA_WORKSPACE_ID = os.getenv("DRATA_WORKSPACE_ID", "").strip()
DRATA_DATASOURCE_ID = os.getenv("DRATA_DATASOURCE_ID", "").strip()

CSV_PATH = os.getenv("TRAINING_CSV_PATH", "training_completions.csv").strip()

APP_AUTH_TOKEN = os.getenv("APP_AUTH_TOKEN", "").strip()

DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def require_auth():
    if not APP_AUTH_TOKEN:
        return
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {APP_AUTH_TOKEN}":
        return jsonify({"error": "Unauthorized"}), 401


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_record(rec):
    email = rec.get("employee_email", "").strip().lower()
    if not email:
        raise ValueError("Missing employee_email")

    status = rec.get("status", "").strip()
    completed_raw = rec.get("completed_at", "").strip()
    expiration_raw = rec.get("expiration_date", "").strip()

    completed_iso = None
    if completed_raw and completed_raw != "-":
        completed_iso = parse_date_to_iso(completed_raw)

    expiration_iso = None
    if expiration_raw and expiration_raw != "-":
        expiration_iso = parse_date_to_iso(expiration_raw)

    return {
        "employee_email": email,
        "employee_name": rec.get("employee_name", ""),
        "training_name": "APS Security Awareness Training",
        "status": status,
        "completed_at": completed_iso,
        "expiration_date": expiration_iso,
        "proof_text": f"{status} on {completed_raw}" if completed_raw else status,
        "source": "APS Security Training Import",
    }


def parse_date_to_iso(value):
    formats = [
        "%b %d %Y",
        "%m/%d/%Y",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value.strip(), fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat()
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


def save_records_to_db(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not records:
        return {"inserted": 0, "updated": 0}

    conn = get_db()
    cur = conn.cursor()
    inserted = 0
    updated = 0

    for rec in records:
        cur.execute("""
            INSERT INTO training_completions
                (employee_email, employee_name, training_name, status,
                 completed_at, expiration_date, proof_text, source, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (employee_email, training_name, completed_at)
            DO UPDATE SET
                employee_name = EXCLUDED.employee_name,
                status = EXCLUDED.status,
                expiration_date = EXCLUDED.expiration_date,
                proof_text = EXCLUDED.proof_text,
                source = EXCLUDED.source,
                synced_at = NOW()
            RETURNING (xmax = 0) AS is_insert
        """, (
            rec["employee_email"],
            rec.get("employee_name", ""),
            rec["training_name"],
            rec.get("status", ""),
            rec.get("completed_at"),
            rec.get("expiration_date"),
            rec.get("proof_text", ""),
            rec.get("source", ""),
        ))
        row = cur.fetchone()
        if row and row[0]:
            inserted += 1
        else:
            updated += 1

    cur.close()
    conn.close()
    return {"inserted": inserted, "updated": updated}


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

    db_result = save_records_to_db(normalized)

    return jsonify({
        "imported": len(normalized),
        "db": db_result,
        "errors": errors,
        "normalized_preview": normalized[:5],
    })


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

    db_result = save_records_to_db(normalized)

    try:
        drata_result = push_to_drata_custom_connection(normalized)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "normalized": len(normalized), "db": db_result, "errors": errors}), 500

    return jsonify({
        "ok": drata_result.get("ok", False),
        "normalized": len(normalized),
        "db": db_result,
        "errors": errors,
        "drata": drata_result,
    })


@app.get("/records")
def get_records():
    maybe = require_auth()
    if maybe:
        return maybe

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT id, employee_email, employee_name, training_name, status,
               completed_at, expiration_date, proof_text, source, synced_at, created_at
        FROM training_completions
        ORDER BY created_at DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    records = []
    for row in rows:
        record = dict(row)
        for key in ["completed_at", "expiration_date", "synced_at", "created_at"]:
            if record.get(key) and hasattr(record[key], "isoformat"):
                record[key] = record[key].isoformat()
        records.append(record)

    return jsonify({"count": len(records), "records": records})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
