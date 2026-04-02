# Automatic Tool Changer (ATC)

A lightweight operator dashboard for managing credentials (tokens), scheduling tool runs, and firing webhooks into a [Tracecat](https://tracecat.com) workflow.

![automatic-tool-changer-v1.png](./automatic-tool-changerv1.png)

## What it does

- **Tokens** — store credentials or other values used by tools (e.g. Confluence API keys)
- **Rules** — auto-run a tool whenever a matching token arrives
- **Runs** — each rule match or manual trigger creates a run that builds the CLI command
- **Campaigns** — isolate tokens, rules, and runs per customer / engagement
- **Webhooks** — fire the built command into a Tracecat webhook; receive results via callback
- **Webhook log** — every outbound webhook call is logged (status code, response, errors) with real-time toast notifications

## Architecture

```
Browser (single-page app)
    │  WebSocket (real-time updates)
    │  REST API
    ▼
FastAPI (main.py)
    │
    ├── SQLAlchemy async ORM
    │       ├── PostgreSQL (production)
    │       └── SQLite   (dev / fallback)
    │
    └── httpx (outbound webhook calls to Tracecat)
```

## Quick start

### Docker (recommended)

```bash
docker compose up --build
```

The app is available at `http://localhost:8000`.
PostgreSQL data is persisted in the `postgres_data` Docker volume.

### Local dev (SQLite)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

uvicorn main:app --reload
```

SQLite database is written to `./atc.db`.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite+aiosqlite:///./atc.db` | SQLAlchemy async connection string |
| `PUBLIC_BASE_URL` | `http://localhost:8000/` | Base URL used to build the `callback_url` sent to Tracecat |

For PostgreSQL:
```
DATABASE_URL=postgresql+asyncpg://atc:atc@localhost:5432/atc
```

## Campaigns

Campaigns are the top-level isolation unit. Each campaign has its own:
- Tokens
- Auto-run rules (watchers)
- Run history
- Webhook configuration (URL, auth header name, secret)

The active campaign is selected from the dropdown in the header. Only one campaign can be active at a time.

## Tokens

A token is a named value — a string, number, JSON object, or credential set — scoped to a campaign.

**Credential object** tokens are expected by tools like `confluence_exporter` and must be a JSON object with:

```json
{
  "url":       "https://yourcompany.atlassian.net/wiki",
  "email":     "you@yourcompany.com",
  "api_token": "YOUR_API_TOKEN",
  "auth_type": "basic"
}
```

Tokens can be added via the UI or the API:

```bash
curl -X POST http://localhost:8000/add_token \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "acme-confluence",
    "type": "credential_object",
    "value": {
      "url": "https://acme.atlassian.net/wiki",
      "email": "ops@acme.com",
      "api_token": "ATATT...",
      "auth_type": "basic"
    }
  }'
```

## Auto-run rules (watchers)

A rule watches for tokens of a given type and automatically creates a run when one arrives. Configure the tool, token type filter, and parameters in the **+ Add Rule** modal.

Wildcard type (`*`) matches any token.

## Webhook integration (Tracecat)

Each campaign can have a webhook URL, auth header name, and secret configured. When a run completes:

- **Auto runs** (triggered by a rule) — webhook fires automatically
- **Manual runs** — a **▶ Fire** button appears on the completed run card

### Payload sent to Tracecat

```json
{
  "commands":     ["CONFLUENCE_URL=... confluence-exporter --space DEV ..."],
  "run_id":       "abc12345",
  "campaign_id":  "xyz98765",
  "callback_url": "http://your-atc-host/runs/abc12345/callback"
}
```

### Callback from Tracecat

Tracecat posts the result back to the `callback_url`:

```bash
# Success
POST /runs/{run_id}/callback
{"result": {"status": "success", "pages": 42}}

# Failure
POST /runs/{run_id}/callback
{"error": "Tracecat workflow failed: ..."}
```

### Auth header

The campaign's **Auth Header Name** (e.g. `Authorization`) and **Secret** are sent as a request header on every outbound webhook call. Configure these to match what your Tracecat webhook endpoint expects.

## Webhook log

Every outbound webhook call is recorded with:
- Timestamp
- Target URL
- HTTP status code
- Response body (up to 500 chars)
- Error message (on connection failure or non-2xx response)

Click **Webhook Log** in the header to view the call history. Failed calls show a red badge count and trigger a toast notification.

## API reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/campaigns` | Create a campaign |
| `GET` | `/campaigns` | List all campaigns |
| `PATCH` | `/campaigns/{id}/activate` | Set active campaign |
| `PATCH` | `/campaigns/{id}` | Update campaign |
| `DELETE` | `/campaigns/{id}` | Delete campaign |
| `POST` | `/add_token` | Add a token |
| `GET` | `/tokens` | List tokens (active campaign) |
| `PATCH` | `/tokens/{id}` | Update a token |
| `POST` | `/watchers` | Create a rule |
| `GET` | `/watchers` | List rules (active campaign) |
| `PATCH` | `/watchers/{id}/toggle` | Pause / resume a rule |
| `PATCH` | `/watchers/{id}` | Update a rule |
| `DELETE` | `/watchers/{id}` | Delete a rule |
| `GET` | `/tools` | List available tools |
| `POST` | `/runs` | Create a manual run |
| `GET` | `/runs` | List runs (active campaign) |
| `POST` | `/runs/{id}/trigger` | Manually fire webhook for a run |
| `POST` | `/runs/{id}/callback` | Receive Tracecat result callback |
| `GET` | `/webhook-logs` | List webhook call log (active campaign) |
| `WS` | `/ws` | Real-time updates |

## Running tests

```bash
pytest
```

Tests use an isolated SQLite database (`test.db`) and reset between each test.
