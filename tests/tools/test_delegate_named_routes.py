"""Configured native routes stay isolated and fail before credential resolution."""

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml

import tools.delegate_tool as dt
from tests.tools.test_delegate import _make_mock_parent
from tools.delegate_tool_config import _resolve_named_route, _valid_named_routes


ROUTES = {
    "flash": {"provider": "openrouter", "model": "deepseek/deepseek-v4-flash-0731", "reasoning_effort": "max"},
    "semantic": {"provider": "openrouter", "model": "qwen/qwen3.8-27b", "reasoning_effort": "xhigh"},
}


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    import run_agent
    import tools.delegate_tool_dispatch as dispatch

    cfg = {"routes": copy.deepcopy(ROUTES), "max_iterations": 7, "max_spawn_depth": 2, "reasoning_effort": "low"}
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("HERMES_IGNORE_USER_CONFIG", raising=False)
    (tmp_path / "config.yaml").write_text(yaml.safe_dump({"delegation": cfg}), encoding="utf-8")
    parent = _make_mock_parent(depth=1)
    parent.session_id = "named-route-parent"
    parent.enabled_toolsets = ["terminal", "file"]
    parent.disabled_toolsets = ["web"]
    parent.reasoning_config = {"enabled": True, "effort": "medium"}
    parent.provider_require_parameters = True
    parent.request_overrides = {}
    parent.max_tokens = 512
    parent.acp_command = None
    parent.acp_args = []
    parent._credential_pool = None
    parent._fallback_chain = [{"provider": "openrouter", "model": "parent/fallback"}]
    parent._interrupt_requested = False

    resolver = MagicMock(return_value={
        "provider": "openrouter", "base_url": "https://openrouter.ai/api/v1",
        "api_key": "test-route-key", "api_mode": "chat_completions",
    })
    monkeypatch.setattr("hermes_cli.runtime_provider.resolve_runtime_provider", resolver)
    monkeypatch.setattr(dt, "_resolve_child_credential_pool", lambda *args: None)
    agent_class = run_agent.AIAgent

    def child(**kwargs):
        instance = MagicMock()
        for key, value in kwargs.items():
            setattr(instance, key, value)
        instance._session_init_model_config = {}
        return instance

    constructor = MagicMock(side_effect=child)
    monkeypatch.setattr(run_agent, "AIAgent", constructor)
    batches = []

    def finish(batch, background=False):
        batches.append((batch, background))
        return {"mode": "background" if background else "sync"}

    monkeypatch.setattr(dispatch, "_execute_and_aggregate", finish)
    monkeypatch.setattr(dispatch, "_dispatch_background", lambda batch: json.dumps(finish(batch, True)))
    return SimpleNamespace(cfg=cfg, parent=parent, resolver=resolver, constructor=constructor,
                           batches=batches, agent_class=agent_class)


@pytest.mark.parametrize("route", list(ROUTES))
@pytest.mark.parametrize("entrypoint,background", [("direct", False), ("direct", True), ("registry", False), ("agent", True)])
def test_selected_route_reaches_child_construction(runtime, route, entrypoint, background):
    from agent.chat_completion_helpers import _provider_preferences_for_agent

    args = {"tasks": [{"goal": "Inspect fixture"}, {"goal": "Check fixture"}], "route": route, "max_iterations": 99}
    if entrypoint == "direct":
        result = dt.delegate_task(**args, background=background, parent_agent=runtime.parent)
    else:
        runtime.parent._delegate_depth = 0 if background else 1
        if entrypoint == "registry":
            result = dt.registry.dispatch("delegate_task", args, parent_agent=runtime.parent)
        else:
            result = runtime.agent_class._dispatch_delegate_task(runtime.parent, args)

    assert json.loads(result)["mode"] == ("background" if background else "sync")
    runtime.resolver.assert_called_once_with(requested="openrouter", target_model=ROUTES[route]["model"])
    assert runtime.constructor.call_count == 2
    batch, selected_background = runtime.batches[0]
    assert selected_background is background
    for call, (_, _, child) in zip(runtime.constructor.call_args_list, batch.children):
        kwargs = call.kwargs
        assert kwargs["provider"] == ROUTES[route]["provider"]
        assert kwargs["model"] == ROUTES[route]["model"]
        assert kwargs["reasoning_config"] == {"enabled": True, "effort": ROUTES[route]["reasoning_effort"]}
        assert kwargs["fallback_model"] is None
        assert _provider_preferences_for_agent(SimpleNamespace(**kwargs))["require_parameters"] is True
        assert kwargs["max_iterations"] == runtime.cfg["max_iterations"]
        assert kwargs["max_tokens"] == runtime.parent.max_tokens
        assert kwargs["iteration_budget"] is None
        assert {"terminal", "file"} <= set(kwargs["enabled_toolsets"])
        assert "web" in kwargs["disabled_toolsets"]
        assert child._delegate_depth == runtime.parent._delegate_depth + 1
    assert dt._load_config()["routes"] == ROUTES


@pytest.mark.parametrize("routes,expected", [
    (None, []), ({}, []), ([], []), ("broken", []),
    ({" flash ": ROUTES["flash"]}, []),
    ({7: ROUTES["flash"]}, []),
    ({"invalid": {**ROUTES["flash"], "base_url": "https://invalid.example"}}, []),
    ({**ROUTES, "invalid": {"model": "missing-fields"}}, ["flash", "semantic"]),
])
def test_schema_exposes_only_valid_routes(tmp_path, monkeypatch, routes, expected):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("HERMES_IGNORE_USER_CONFIG", raising=False)
    cfg = {} if routes is None else {"routes": routes}
    (tmp_path / "config.yaml").write_text(yaml.safe_dump({"delegation": cfg}), encoding="utf-8")
    original = copy.deepcopy(dt.DELEGATE_TASK_SCHEMA)
    schema = dt.registry.get_definitions({"delegate_task"})[0]["function"]
    properties = schema["parameters"]["properties"]
    if expected:
        assert properties["route"]["enum"] == expected
        properties["route"]["enum"].append("injected")
        assert dt._build_dynamic_schema_overrides()["parameters"]["properties"]["route"]["enum"] == expected
    else:
        assert "route" not in properties
    assert dt.DELEGATE_TASK_SCHEMA == original
    assert "route" not in original["parameters"]["properties"]


INVALID_SELECTIONS = [
    (name, ROUTES) for name in ("", " ", " flash", "semantic ", "FLASH", "missing", 1, False, [], {})
] + [
    ("flash", section) for section in (None, [], [ROUTES["flash"]], "flash", 1, True)
] + [
    ("flash", {"flash": entry}) for entry in (None, [], "flash", {}, {"provider": "openrouter"})
] + [
    ("flash", {"flash": {**ROUTES["flash"], key: value}})
    for key in ("provider", "model") for value in (None, "", " ", " padded ", 123, [], {}, False)
] + [
    ("flash", {"flash": {**ROUTES["flash"], "reasoning_effort": effort}})
    for effort in (None, True, 0, 1, [], {}, "unknown")
] + [
    ("flash", {"flash": {**ROUTES["flash"], key: "injected"}})
    for key in ("base_url", "api_key", "profile", "request_overrides", "fallback_providers")
] + [(" flash ", {" flash ": ROUTES["flash"]})]


@pytest.mark.parametrize("selection,routes", INVALID_SELECTIONS)
def test_invalid_selection_never_resolves_credentials_or_builds_children(runtime, monkeypatch, selection, routes):
    monkeypatch.setattr(dt, "_load_config", lambda: {"routes": routes})
    credentials = MagicMock()
    monkeypatch.setattr(dt, "_resolve_delegation_credentials", credentials)
    result = json.loads(dt.delegate_task(goal="Check fixture", route=selection, parent_agent=runtime.parent))
    assert "error" in result
    credentials.assert_not_called()
    runtime.resolver.assert_not_called()
    runtime.constructor.assert_not_called()
    assert not runtime.batches


@pytest.mark.parametrize("entrypoint", ["registry", "agent"])
def test_entrypoints_forward_exact_selection(runtime, monkeypatch, entrypoint):
    delegate = MagicMock(return_value="{}")
    monkeypatch.setattr(dt, "delegate_task", delegate)
    args = {"goal": "Check fixture", "route": " semantic "}
    if entrypoint == "registry":
        dt.registry.dispatch("delegate_task", args, parent_agent=runtime.parent)
    else:
        runtime.agent_class._dispatch_delegate_task(runtime.parent, args)
    assert delegate.call_args.kwargs["route"] == args["route"]


@pytest.mark.parametrize("auxiliary", [False, True])
def test_omitted_route_preserves_legacy_reasoning_and_fallback(runtime, auxiliary):
    credentials_cfg = copy.deepcopy(ROUTES["semantic"]) if auxiliary else None
    dt.delegate_task(goal="Legacy child", parent_agent=runtime.parent, credentials_cfg=credentials_cfg)
    kwargs = runtime.constructor.call_args.kwargs
    assert kwargs["reasoning_config"] == {"enabled": True, "effort": runtime.cfg["reasoning_effort"]}
    assert kwargs["model"] == (ROUTES["semantic"]["model"] if auxiliary else runtime.parent.model)
    assert kwargs["fallback_model"] == (None if auxiliary else runtime.parent._fallback_chain)
    if not auxiliary:
        runtime.resolver.assert_not_called()


def test_successive_selections_do_not_change_defaults_or_siblings(runtime):
    for route in ("flash", "semantic", None):
        dt.delegate_task(goal="Independent child", route=route, parent_agent=runtime.parent)
    first, second, default = [call.kwargs for call in runtime.constructor.call_args_list]
    first["reasoning_config"]["effort"] = "mutated"
    assert second["reasoning_config"]["effort"] == "xhigh"
    assert default["reasoning_config"]["effort"] == "low"
    assert default["model"] == runtime.parent.model
    assert runtime.parent.reasoning_config["effort"] == "medium"
    assert dt._load_config()["routes"] == ROUTES


def test_concurrent_route_resolution_returns_independent_selections():
    cfg = {"routes": copy.deepcopy(ROUTES)}
    barrier = Barrier(4)

    def resolve(name):
        selected = _resolve_named_route(name, cfg)
        barrier.wait(timeout=5)
        original = dict(selected)
        selected["model"] = "mutated"
        return original

    names = ["flash", "semantic", "flash", "semantic"]
    with ThreadPoolExecutor(max_workers=4) as pool:
        selections = list(pool.map(resolve, names))
    assert selections == [ROUTES[name] for name in names]
    assert cfg["routes"] == ROUTES
    assert _valid_named_routes(cfg) == ROUTES


def test_explicit_disabled_reasoning_is_preserved(runtime, monkeypatch):
    cfg = {**runtime.cfg, "routes": {"off": {**ROUTES["flash"], "reasoning_effort": False}}}
    monkeypatch.setattr(dt, "_load_config", lambda: cfg)
    dt.delegate_task(goal="Child without reasoning", route="off", parent_agent=runtime.parent)
    assert runtime.constructor.call_args.kwargs["reasoning_config"] == {"enabled": False}
