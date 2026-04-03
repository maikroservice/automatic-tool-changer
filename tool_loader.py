"""
Tool loader — discovers YAML-defined CLI tools and builds commands.

YAML schema
-----------
id: tool_id
name: Human Name
description: >-
  Multi-line description shown in the UI.
command: cli-binary

# Required env vars — map ENV_VAR to credential field names (first non-empty wins).
env:
  MY_URL:   [url, tool_url]
  MY_TOKEN: [token, api_token]

# Optional env vars — same resolution but omit when value is empty or in omit_if.
optional_env:
  MY_TYPE:
    from: [my_type]
    omit_if: ["", default_value]

# CLI argument list — processed in order.
#
#   flag     --flag value           always included (unless omit_if_empty: true)
#   boolean  --flag                 included only when param is truthy
#   scope    --{param_value} {value_param}
#            e.g. scope=project, scope_value=PROJ  →  --project PROJ
#
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

# UI parameter definitions (same structure the frontend expects)
parameters:
  - name: scope
    type: select
    label: Scope
    options: [space, page, recursive]
    default: space
    required: true
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_tools(directory: str = "tools") -> tuple[dict[str, dict], dict[str, dict]]:
    """
    Load all .yaml files from *directory*.

    Returns (api_tools, full_tools) where:
      api_tools  — {id: api_metadata}   safe to serialise and send to the frontend
      full_tools — {id: full_config}    used internally for command building
    """
    api_tools: dict[str, dict] = {}
    full_tools: dict[str, dict] = {}
    tools_dir = Path(directory)
    if not tools_dir.exists():
        return api_tools, full_tools
    for path in sorted(tools_dir.glob("*.yaml")):
        config = _load_yaml(path)
        if not config or "id" not in config:
            continue
        tid = config["id"]
        api_tools[tid] = {
            "id":          tid,
            "name":        config.get("name", tid),
            "description": config.get("description", ""),
            "parameters":  config.get("parameters", []),
        }
        full_tools[tid] = config
    return api_tools, full_tools


def build_commands(tool: dict, values: list[Any], params: dict) -> list[str]:
    """Build one CLI command string per credential value."""
    return [build_command(tool, v, params) for v in values]


def build_command(tool: dict, cred: Any, params: dict) -> str:
    """Build a single CLI command string for one credential value."""
    cred_dict = cred if isinstance(cred, dict) else {}

    # ── Env vars ─────────────────────────────────────────────────────────────
    env_parts: list[str] = []

    for var, keys in (tool.get("env") or {}).items():
        val = _resolve(cred_dict, _as_list(keys))
        if val is not None:
            env_parts.append(f"{var}={val}")

    for var, spec in (tool.get("optional_env") or {}).items():
        if isinstance(spec, (str, list)):
            keys, omit = _as_list(spec), []
        else:
            keys = _as_list(spec.get("from", []))
            omit = [str(o) for o in _as_list(spec.get("omit_if", []))]
        val = _resolve(cred_dict, keys)
        if val is not None and str(val) not in omit:
            env_parts.append(f"{var}={val}")

    # ── CLI args ──────────────────────────────────────────────────────────────
    arg_parts: list[str] = [tool.get("command", "")]

    for arg_spec in (tool.get("args") or []):
        arg_type = arg_spec.get("type", "flag")

        if arg_type == "scope":
            scope_val = str(params.get(arg_spec["param"], ""))
            value_val = str(params.get(arg_spec["value_param"], ""))
            if scope_val:
                arg_parts.append(f"--{scope_val} {value_val}")

        elif arg_type == "flag":
            flag  = arg_spec["flag"]
            value = params.get(arg_spec["param"], "")
            if arg_spec.get("omit_if_empty") and not value:
                continue
            if value != "" and value is not None:
                arg_parts.append(f"{flag} {value}")

        elif arg_type == "boolean":
            flag  = arg_spec["flag"]
            value = params.get(arg_spec["param"], False)
            if value in (True, "true", "on", "yes", "1", 1):
                arg_parts.append(flag)

    env_str  = " ".join(env_parts)
    args_str = " ".join(arg_parts)
    return f"{env_str} {args_str}".strip()


def validate_tool(config: dict) -> list[str]:
    """
    Basic structural validation.
    Returns a list of error strings (empty list means valid).
    """
    errors: list[str] = []
    if not isinstance(config, dict):
        return ["tool definition must be a YAML mapping"]
    for field in ("id", "name", "command"):
        if not config.get(field):
            errors.append(f"missing required field: {field!r}")
    for i, p in enumerate(config.get("parameters") or []):
        for f in ("name", "type", "label"):
            if not p.get(f):
                errors.append(f"parameter[{i}] missing field {f!r}")
    return errors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> dict | None:
    with open(path) as f:
        return yaml.safe_load(f)


def _as_list(val: Any) -> list:
    if isinstance(val, list):
        return val
    if val is None:
        return []
    return [val]


def _resolve(cred_dict: dict, keys: list[str]) -> Any:
    """Return the first non-empty value found in cred_dict for any key."""
    for key in keys:
        val = cred_dict.get(key)
        if val is not None and val != "":
            return val
    return None
