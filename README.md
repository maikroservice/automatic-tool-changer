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
    ├── tool_loader.py — loads tools from tools/*.yaml
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
| `APP_TOKEN` | *(auto-generated)* | Token required by privileged endpoints (`POST/DELETE /tools`). Auto-generated at startup if not set. |
| `TOOLS_DIR` | `tools` | Directory where tool YAML definitions are loaded from |

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

## Tools

Tools are defined as YAML files in the `tools/` directory. Each file describes the tool metadata, how to map credential fields to env vars, and how to assemble the CLI command.

### Tool YAML schema

```yaml
id: my_tool                  # unique identifier, also used as the filename
name: My Tool                # display name in the UI
description: >-              # shown in the UI
  What this tool does.
command: my-tool-binary      # the executable to call
token_type: credential_object  # hint shown in the UI (optional)

# Required env vars — map ENV_VAR to credential field names.
# The first non-empty field wins.
env:
  MY_URL:   [url, my_url]
  MY_TOKEN: [token, api_token]

# Optional env vars — same resolution but omit if empty or matches omit_if.
optional_env:
  MY_TYPE:
    from: [my_type]
    omit_if: ["", default_value]

# CLI argument list — processed in order.
# Types:
#   flag     --flag value           always included (unless omit_if_empty: true)
#   boolean  --flag                 included only when param is truthy
#   scope    --{param_value} {value_param}
#            e.g. scope=project, scope_value=PROJ  →  --project PROJ
args:
  - type: scope
    param: scope
    value_param: scope_value
  - type: flag
    flag: --format
    param: format
  - type: flag
    flag: --output
    param: output_dir
    omit_if_empty: true
  - type: boolean
    flag: --force
    param: force

# Parameter definitions — same structure the frontend uses to render the form.
parameters:
  - name: scope
    type: select          # select | text | checkbox | textarea
    label: Scope
    options: [issue, project]
    default: project
    required: true
  - name: force
    type: checkbox
    label: Force overwrite
    default: false
    required: false
```

### Registering a new tool

Drop a `.yaml` file into `tools/` and restart, **or** use the UI:

1. Click **+ Add Tool** in the header
2. Choose your `.yaml` file
3. The tool appears immediately in all tool dropdowns for all connected browsers

Removing a tool: call `DELETE /tools/{id}` from the UI (the delete button appears in a future tools management panel), or remove the file from `tools/` and restart.

### Built-in tools

| File | Tool | Command |
|---|---|---|
| `tools/confluence_exporter.yaml` | Confluence Exporter | `confluence-exporter` |
| `tools/jira_exporter.yaml` | Jira Exporter | `jira-export` |

## API reference

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/campaigns` | — | Create a campaign |
| `GET` | `/campaigns` | — | List all campaigns |
| `PATCH` | `/campaigns/{id}/activate` | — | Set active campaign |
| `PATCH` | `/campaigns/{id}` | — | Update campaign |
| `DELETE` | `/campaigns/{id}` | — | Delete campaign |
| `POST` | `/add_token` | — | Add a token |
| `GET` | `/tokens` | — | List tokens (active campaign) |
| `PATCH` | `/tokens/{id}` | — | Update a token |
| `POST` | `/watchers` | — | Create a watcher |
| `GET` | `/watchers` | — | List watchers (active campaign) |
| `PATCH` | `/watchers/{id}/toggle` | — | Pause / resume a watcher |
| `PATCH` | `/watchers/{id}` | — | Update a watcher |
| `DELETE` | `/watchers/{id}` | — | Delete a watcher |
| `GET` | `/tools` | — | List available tools |
| `POST` | `/tools` | `X-ATC-Token` | Upload a tool YAML |
| `DELETE` | `/tools/{id}` | `X-ATC-Token` | Remove a tool |
| `POST` | `/runs` | — | Create a manual run |
| `GET` | `/runs` | — | List runs (active campaign) |
| `POST` | `/runs/{id}/trigger` | — | Manually fire webhook for a run |
| `POST` | `/runs/{id}/callback` | — | Receive Tracecat result callback |
| `GET` | `/webhook-logs` | — | List webhook call log (active campaign) |
| `WS` | `/ws` | — | Real-time updates |

## Running tests

```bash
pytest
```

Tests use an isolated SQLite database (`test.db`) and reset between each test.
