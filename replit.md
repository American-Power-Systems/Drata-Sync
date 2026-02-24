# Drata Security Training Sync

A Flask API that normalizes security training completion records, stores them in PostgreSQL, and pushes them to Drata via a Custom Connection.

## Architecture

Single-file Flask app (`main.py`) with four endpoints:

- `GET /health` — liveness check
- `POST /training/import` — validate, normalize, and store records in DB (no Drata push)
- `POST /sync` — normalize records from CSV and/or inline JSON, store in DB, then push to Drata
- `GET /records` — query all stored training completion records from the database

## Database

PostgreSQL with a `training_completions` table:

- `id` (serial PK)
- `employee_email`, `employee_name`, `training_name`, `status`
- `completed_at`, `expiration_date` (timestamptz)
- `proof_text`, `source`
- `synced_at`, `created_at`
- Unique constraint on `(employee_email, training_name, completed_at)` for upsert behavior

## Configuration (Replit Secrets)

| Secret | Required | Description |
|---|---|---|
| `DATABASE_URL` | Yes (auto-set) | PostgreSQL connection string |
| `DRATA_API_KEY` | Yes | Your Drata API key |
| `DRATA_BASE_URL` | No | Defaults to `https://api.drata.com` |
| `DRATA_WORKSPACE_ID` | Yes (for sync) | Drata workspace ID |
| `DRATA_DATASOURCE_ID` | Yes (for sync) | Drata custom connection datasource ID |
| `TRAINING_CSV_PATH` | No | Path to CSV file, defaults to `training_completions.csv` |
| `APP_AUTH_TOKEN` | No | Bearer token to protect endpoints |

## CSV Format

Columns: `employee_name`, `employee_email`, `status`, `completed_at`, `expiration_date`

Date format: `Jun 23 2025` (no commas)

## Dependencies

- Python 3
- flask
- requests
- psycopg2-binary

## Run

```
python main.py
```

Runs on port 5000.
