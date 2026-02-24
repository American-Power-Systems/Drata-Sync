# Drata Security Training Sync

A Flask API that normalizes security training completion records and pushes them to Drata via a Custom Connection.

## Architecture

Single-file Flask app (`main.py`) with three endpoints:

- `GET /health` — liveness check
- `POST /training/import` — validate and normalize records (dry run, no Drata push)
- `POST /sync` — normalize records from CSV and/or inline JSON, then push to Drata

## Configuration (Replit Secrets)

| Secret | Required | Description |
|---|---|---|
| `DRATA_API_KEY` | Yes | Your Drata API key |
| `DRATA_BASE_URL` | No | Defaults to `https://api.drata.com` |
| `DRATA_WORKSPACE_ID` | Yes (for sync) | Drata workspace ID |
| `DRATA_DATASOURCE_ID` | Yes (for sync) | Drata custom connection datasource ID |
| `TRAINING_CSV_PATH` | No | Path to CSV file, defaults to `training_completions.csv` |
| `APP_AUTH_TOKEN` | No | Bearer token to protect endpoints |

## CSV Format

Minimum columns: `employee_email`, `completed_at`

Recommended: `training_name`, `proof_url`, `status`

## Record Schema

```json
{
  "employee_email": "user@company.com",
  "training_name": "Security Awareness Training",
  "completed_at": "2026-02-24T18:20:00Z",
  "status": "completed",
  "proof_url": "https://example.com/cert.pdf"
}
```

## Dependencies

- Python 3
- flask
- requests

## Run

```
python main.py
```

Runs on port 5000.
