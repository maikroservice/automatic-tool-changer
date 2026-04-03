"""Unit tests for execute_tool — jira_exporter and confluence_exporter."""
import pytest
from main import execute_tool

CREDS = {
    "url": "https://company.atlassian.net/wiki",
    "email": "user@company.com",
    "api_token": "TOKEN123",
    "auth_type": "basic",
}


def test_confluence_exporter_builds_command():
    result = execute_tool("confluence_exporter", [CREDS], {"scope": "space", "scope_value": "DEV", "format": "md", "output_dir": "./out"})
    assert len(result) == 1
    cmd = result[0]
    assert "confluence-exporter" in cmd
    assert "--space DEV" in cmd
    assert "--format md" in cmd
    assert "--output ./out" in cmd


def test_confluence_exporter_injects_credentials():
    result = execute_tool("confluence_exporter", [CREDS], {"scope": "space", "scope_value": "DEV", "format": "md"})
    cmd = result[0]
    assert "CONFLUENCE_URL=https://company.atlassian.net/wiki" in cmd
    assert "CONFLUENCE_EMAIL=user@company.com" in cmd
    assert "CONFLUENCE_TOKEN=TOKEN123" in cmd
    assert "CONFLUENCE_AUTH_TYPE" not in cmd  # basic is the default, omitted


def test_confluence_exporter_page_scope():
    result = execute_tool("confluence_exporter", [CREDS], {"scope": "page", "scope_value": "123456", "format": "html"})
    cmd = result[0]
    assert "--page 123456" in cmd
    assert "--format html" in cmd


def test_confluence_exporter_recursive_with_depth():
    result = execute_tool("confluence_exporter", [CREDS], {"scope": "recursive", "scope_value": "123456", "format": "md", "depth": "3"})
    cmd = result[0]
    assert "--recursive 123456" in cmd
    assert "--depth 3" in cmd


def test_confluence_exporter_force_flag():
    result = execute_tool("confluence_exporter", [CREDS], {"scope": "space", "scope_value": "DEV", "format": "md", "force": "true"})
    assert "--force" in result[0]


def test_confluence_exporter_no_force_by_default():
    result = execute_tool("confluence_exporter", [CREDS], {"scope": "space", "scope_value": "DEV", "format": "md"})
    assert "--force" not in result[0]


def test_confluence_exporter_no_depth_when_empty():
    result = execute_tool("confluence_exporter", [CREDS], {"scope": "space", "scope_value": "DEV", "format": "md", "depth": ""})
    assert "--depth" not in result[0]


def test_confluence_exporter_empty_values_list():
    assert execute_tool("confluence_exporter", [], {}) == []


def test_confluence_exporter_multiple_credential_tokens():
    creds2 = {**CREDS, "url": "https://other.atlassian.net/wiki", "email": "other@company.com"}
    result = execute_tool("confluence_exporter", [CREDS, creds2], {"scope": "space", "scope_value": "DEV", "format": "md"})
    assert len(result) == 2
    assert "company.atlassian.net" in result[0]
    assert "other.atlassian.net" in result[1]


def test_confluence_exporter_non_dict_token():
    result = execute_tool("confluence_exporter", ["not-a-dict"], {"scope": "space", "scope_value": "DEV", "format": "md"})
    assert len(result) == 1
    assert "confluence-exporter" in result[0]


def test_confluence_exporter_alternate_key_names():
    creds_alt = {"confluence_url": "https://alt.atlassian.net/wiki", "confluence_email": "a@b.com", "confluence_token": "TOK"}
    result = execute_tool("confluence_exporter", [creds_alt], {"scope": "space", "scope_value": "X", "format": "md"})
    assert "CONFLUENCE_URL=https://alt.atlassian.net/wiki" in result[0]
    assert "CONFLUENCE_EMAIL=a@b.com" in result[0]
    assert "CONFLUENCE_TOKEN=TOK" in result[0]


def test_unknown_tool_raises_value_error():
    with pytest.raises(ValueError, match="Unknown tool"):
        execute_tool("nonexistent", ["x"], {})


# ── jira_exporter ────────────────────────────────────────────────────────────

JIRA_CREDS = {
    "url":   "https://company.atlassian.net",
    "email": "user@company.com",
    "token": "JIRATOKEN123",
}


def test_jira_exporter_builds_command():
    result = execute_tool("jira_exporter", [JIRA_CREDS], {"scope": "project", "scope_value": "PROJ", "format": "md", "output_dir": "./export"})
    assert len(result) == 1
    cmd = result[0]
    assert "jira-export" in cmd
    assert "--project PROJ" in cmd
    assert "--format md" in cmd
    assert "--output ./export" in cmd


def test_jira_exporter_injects_credentials():
    result = execute_tool("jira_exporter", [JIRA_CREDS], {"scope": "project", "scope_value": "PROJ", "format": "md"})
    cmd = result[0]
    assert "JIRA_URL=https://company.atlassian.net" in cmd
    assert "JIRA_EMAIL=user@company.com" in cmd
    assert "JIRA_TOKEN=JIRATOKEN123" in cmd


def test_jira_exporter_issue_scope():
    result = execute_tool("jira_exporter", [JIRA_CREDS], {"scope": "issue", "scope_value": "PROJ-123", "format": "md"})
    assert "--issue PROJ-123" in result[0]


def test_jira_exporter_board_scope():
    result = execute_tool("jira_exporter", [JIRA_CREDS], {"scope": "board", "scope_value": "42", "format": "md"})
    assert "--board 42" in result[0]


def test_jira_exporter_jql_scope():
    result = execute_tool("jira_exporter", [JIRA_CREDS], {"scope": "jql", "scope_value": "project = PROJ", "format": "raw"})
    cmd = result[0]
    assert "--jql project = PROJ" in cmd
    assert "--format raw" in cmd


def test_jira_exporter_comments_flag():
    result = execute_tool("jira_exporter", [JIRA_CREDS], {"scope": "project", "scope_value": "PROJ", "format": "md", "comments": True})
    assert "--comments" in result[0]


def test_jira_exporter_no_comments_by_default():
    result = execute_tool("jira_exporter", [JIRA_CREDS], {"scope": "project", "scope_value": "PROJ", "format": "md"})
    assert "--comments" not in result[0]


def test_jira_exporter_force_flag():
    result = execute_tool("jira_exporter", [JIRA_CREDS], {"scope": "project", "scope_value": "PROJ", "format": "md", "force": True})
    assert "--force" in result[0]


def test_jira_exporter_server_type():
    creds = {**JIRA_CREDS, "jira_type": "server"}
    result = execute_tool("jira_exporter", [creds], {"scope": "project", "scope_value": "PROJ", "format": "md"})
    assert "JIRA_TYPE=server" in result[0]


def test_jira_exporter_cloud_type_not_set():
    """cloud is the default — JIRA_TYPE should not appear in the command."""
    creds = {**JIRA_CREDS, "jira_type": "cloud"}
    result = execute_tool("jira_exporter", [creds], {"scope": "project", "scope_value": "PROJ", "format": "md"})
    assert "JIRA_TYPE" not in result[0]


def test_jira_exporter_bearer_auth():
    creds = {**JIRA_CREDS, "auth_type": "bearer"}
    result = execute_tool("jira_exporter", [creds], {"scope": "project", "scope_value": "PROJ", "format": "md"})
    assert "JIRA_AUTH_TYPE=bearer" in result[0]


def test_jira_exporter_basic_auth_not_set():
    """basic is the default — JIRA_AUTH_TYPE should not appear."""
    creds = {**JIRA_CREDS, "auth_type": "basic"}
    result = execute_tool("jira_exporter", [creds], {"scope": "project", "scope_value": "PROJ", "format": "md"})
    assert "JIRA_AUTH_TYPE" not in result[0]


def test_jira_exporter_alternate_key_names():
    creds_alt = {"jira_url": "https://alt.atlassian.net", "jira_email": "a@b.com", "api_token": "TOK"}
    result = execute_tool("jira_exporter", [creds_alt], {"scope": "project", "scope_value": "X", "format": "md"})
    assert "JIRA_URL=https://alt.atlassian.net" in result[0]
    assert "JIRA_EMAIL=a@b.com" in result[0]
    assert "JIRA_TOKEN=TOK" in result[0]


def test_jira_exporter_multiple_tokens():
    creds2 = {**JIRA_CREDS, "url": "https://other.atlassian.net", "email": "other@company.com"}
    result = execute_tool("jira_exporter", [JIRA_CREDS, creds2], {"scope": "project", "scope_value": "PROJ", "format": "md"})
    assert len(result) == 2
    assert "company.atlassian.net" in result[0]
    assert "other.atlassian.net" in result[1]


def test_jira_exporter_empty_values_list():
    assert execute_tool("jira_exporter", [], {}) == []
