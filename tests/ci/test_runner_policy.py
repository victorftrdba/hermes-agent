"""Evaluate the runner and capacity contracts declared by the real workflows."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml


_WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"
_UPSTREAM = "NousResearch/hermes-agent"
_REPOSITORIES = [
    _UPSTREAM,
    _UPSTREAM.upper(),
    "victorftrdba/hermes-agent",
    "another-owner/hermes-agent",
    "NousResearch/another-repo",
    "NousResearch/hermes-agent-extra",
    "other-NousResearch/hermes-agent",
    "",
]
_RUNNER_POLICIES = [
    ("tests.yml", "test", None, "ubuntu-latest-96-core", "ubuntu-24.04"),
    ("tests-os.yml", "os-tests", "windows_only", "windows-latest-32-core", "windows-2025"),
    ("nix.yml", "flake-check", None, "ubuntu-latest-32-core", "ubuntu-24.04"),
    ("js-tests.yml", "check", None, "ubuntu-latest-32-core", "ubuntu-24.04"),
    ("rust-tests.yml", "bootstrap-installer", None, "ubuntu-latest-32-core", "ubuntu-24.04"),
    ("e2e-desktop.yml", "e2e", None, "ubuntu-latest-32-core", "ubuntu-24.04"),
]


def _job(workflow: str, job: str) -> dict:
    return yaml.safe_load((_WORKFLOWS / workflow).read_text(encoding="utf-8"))["jobs"][job]


def _evaluate(value: object, repository: str, matrix: dict | None = None) -> object:
    """Resolve the policy's expression subset, including Actions' caseless equality."""
    if not isinstance(value, str) or not value.startswith("${{"):
        return value
    expression = re.fullmatch(r"\$\{\{\s*(.*?)\s*\}\}", value, re.DOTALL)
    assert expression, f"Malformed Actions expression: {value}"
    body = expression.group(1)
    if reference := re.fullmatch(r"matrix\.([A-Za-z_][A-Za-z_0-9]*)", body):
        assert matrix is not None, "Matrix expression requires an actual matrix row"
        return _evaluate(matrix[reference.group(1)], repository, matrix)
    literal = r"(?:'[^']*'|[0-9]+)"
    condition = re.fullmatch(
        rf"github\.repository\s*==\s*'([^']*)'\s*&&\s*({literal})\s*\|\|\s*({literal})",
        body,
    )
    assert condition, f"Unsupported runner-policy expression: {value}"
    expected_repository, upstream, fork = condition.groups()

    def parse_literal(token: str) -> str | int:
        return token[1:-1] if token.startswith("'") else int(token)

    selected = (
        parse_literal(upstream)
        if repository.casefold() == expected_repository.casefold()
        else False
    )
    return selected or parse_literal(fork)


@pytest.mark.parametrize("repository", _REPOSITORIES)
@pytest.mark.parametrize("workflow,job_id,marker,upstream,fork", _RUNNER_POLICIES)
def test_selected_runner_is_available_to_the_repository(repository, workflow, job_id, marker, upstream, fork):
    job = _job(workflow, job_id)
    matrix = None
    if marker:
        matrix = next(row for row in job["strategy"]["matrix"]["include"] if row["marker"] == marker)
    expected = upstream if repository.casefold() == _UPSTREAM.casefold() else fork
    assert _evaluate(job["runs-on"], repository, matrix) == expected


@pytest.mark.parametrize("repository", _REPOSITORIES)
def test_python_workers_fit_the_selected_runner(repository):
    job = _job("tests.yml", "test")
    test_steps = [step for step in job["steps"] if "HERMES_TEST_WORKERS" in step.get("env", {})]
    assert test_steps, "The full Python test job must explicitly bound file concurrency"
    capacity = 96 if repository.casefold() == _UPSTREAM.casefold() else 4
    for step in test_steps:
        assert _evaluate(step["env"]["HERMES_TEST_WORKERS"], repository) == capacity


@pytest.mark.parametrize("repository", _REPOSITORIES)
def test_python_timeout_accounts_for_fork_capacity(repository):
    job = _job("tests.yml", "test")
    expected = 30 if repository.casefold() == _UPSTREAM.casefold() else 60
    assert _evaluate(job["timeout-minutes"], repository) == expected


@pytest.mark.parametrize("repository", _REPOSITORIES)
def test_macos_lane_keeps_its_native_runner(repository):
    job = _job("tests-os.yml", "os-tests")
    matrix = next(row for row in job["strategy"]["matrix"]["include"] if row["marker"] == "macos_only")
    assert _evaluate(job["runs-on"], repository, matrix) == "macos-latest"
