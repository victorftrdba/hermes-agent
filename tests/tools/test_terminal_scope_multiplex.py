"""Per-turn terminal scope isolation under profile multiplexing (#68559 class).

One multiplexed process serves several profiles, but terminal.* used to
resolve through the process-global ``TERMINAL_*`` env vars bridged once at
startup — so every routed profile inherited the launch profile's backend,
cwd, docker mounts and shared-container key (#68559, #94200, #101132,
#95470). ``tools.terminal_scope`` installs the routed profile's COMPLETE
terminal policy as a ContextVar at each profile boundary; readers resolve
ONLY from it (omitted key → defined default, never ``os.environ``) and an
unresolvable policy fails closed.
"""

import json
import os
import threading
import time

import pytest

from tools.terminal_scope import (
    TerminalPolicyRefusal,
    TerminalPolicyUnavailable,
    get_terminal_scope,
    install_profile_terminal_scope,
    reset_terminal_scope,
    set_terminal_scope,
    terminal_env,
)
from tools import browser_tool_cloud as bt_cloud

_LAUNCH_CWD = "/home/launch-user/private"
_LAUNCH_VOLUMES = '["/host/secret:/data:rw"]'


@pytest.fixture(autouse=True)
def _polluted_launch_env(monkeypatch, tmp_path):
    """Launch profile A bridged a docker backend with sensitive policy into
    the process env; every test proves a routed profile observes none of it."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    monkeypatch.setenv("TERMINAL_ENV", "docker")
    monkeypatch.setenv("TERMINAL_CWD", _LAUNCH_CWD)
    monkeypatch.setenv("TERMINAL_DOCKER_VOLUMES", _LAUNCH_VOLUMES)
    monkeypatch.setenv("TERMINAL_DOCKER_SHARED_CONTAINER_KEY", "alpha-shared")
    monkeypatch.setenv("TERMINAL_SSH_HOST", "10.10.0.103")
    monkeypatch.setattr("agent.secret_scope.build_profile_secret_scope", lambda _h: {})
    monkeypatch.setattr("hermes_cli.env_loader.hydrate_profile_secret_sources", lambda _h: None)
    import tools.terminal_tool as tt

    monkeypatch.setattr(tt, "_terminal_config_bridge_attempted", True)
    yield


def _profile(tmp_path, name, config_yaml="", dotenv=""):
    home = tmp_path / "profiles" / name
    home.mkdir(parents=True)
    if config_yaml:
        (home / "config.yaml").write_text(config_yaml, encoding="utf-8")
    if dotenv:
        (home / ".env").write_text(dotenv, encoding="utf-8")
    return home


def test_no_scope_keeps_process_env_behavior():
    """Single-process CLI/TUI (no scope bound) is byte-identical to before."""
    assert terminal_env("TERMINAL_ENV") == "docker"
    assert terminal_env("TERMINAL_SSH_HOST") == "10.10.0.103"


def test_scoped_read_never_falls_through_to_process_env():
    """Omitted key under a scope → defined default, NOT the ambient value."""
    token = set_terminal_scope({"TERMINAL_ENV": "local"})
    try:
        assert terminal_env("TERMINAL_ENV") == "local"
        assert terminal_env("TERMINAL_SSH_HOST") == ""
        assert terminal_env("TERMINAL_DOCKER_VOLUMES", "[]") == "[]"
        assert os.environ["TERMINAL_ENV"] == "docker"  # never mutated
    finally:
        reset_terminal_scope(token)


@pytest.mark.parametrize(
    "config_yaml,dotenv",
    [
        pytest.param("terminal:\n  backend: local\n  cwd: {cwd}\n", "", id="config-yaml"),
        pytest.param("", "TERMINAL_ENV=local\nTERMINAL_CWD={cwd}\n", id="dotenv-only"),
    ],
)
def test_routed_turn_reads_every_terminal_consumer_from_profile(
    tmp_path, config_yaml, dotenv
):
    """Leak matrix through the REAL gateway boundary: a routed local profile
    with its own cwd must be seen as such by every terminal.* consumer —
    terminal_tool config, container key resolution, docker media translation,
    file_tools/runtime_cwd cwd anchors, and the browser/env_probe backend
    checks — with none of launch profile A's docker policy showing through."""
    import gateway.run as gw
    import tools.terminal_tool as tt
    from agent import runtime_cwd
    from gateway.platforms import base as gbase
    from tools import browser_tool, env_probe, file_tools_paths

    b_cwd = tmp_path / "b-work"
    b_cwd.mkdir()
    home = _profile(
        tmp_path, "bee",
        config_yaml.format(cwd=b_cwd), dotenv.format(cwd=b_cwd),
    )

    with gw._profile_runtime_scope(home):
        cfg = tt._get_env_config()
        assert cfg["env_type"] == "local"
        assert cfg["cwd"] == str(b_cwd)
        assert cfg["docker_volumes"] == []
        assert cfg["docker_shared_container_key"] == ""
        assert tt._resolve_container_task_id(None) == "default"
        assert gbase._parse_docker_volume_mounts() == []
        assert not any(
            "alpha-shared" in c for c in gbase._docker_sandbox_dir_candidates("agent:bee:x")
        )
        assert file_tools_paths._configured_terminal_cwd() == str(b_cwd)
        assert runtime_cwd.resolve_agent_cwd() == b_cwd
        assert bt_cloud._is_local_backend() is True
        # env_probe bails out with "" for remote backends; a local profile
        # must not be treated as remote just because the launch env is docker.
        assert env_probe._resolve_terminal_backend() == "local"
    assert get_terminal_scope() is None
    # Process env untouched — the launch profile's own turns are unchanged.
    assert os.environ["TERMINAL_DOCKER_VOLUMES"] == _LAUNCH_VOLUMES


def test_profile_omitting_keys_gets_defaults_not_launch_values(tmp_path):
    """#101132/#95470: a docker profile that does NOT set docker_volumes or
    docker_shared_container_key must not inherit the launch profile's."""
    import gateway.run as gw
    import tools.terminal_tool as tt

    home = _profile(tmp_path, "bee", "terminal:\n  backend: docker\n")
    with gw._profile_runtime_scope(home):
        cfg = tt._get_env_config()
        assert cfg["env_type"] == "docker"
        assert cfg["docker_volumes"] == []
        assert cfg["docker_shared_container_key"] == ""
        assert cfg["ssh_host"] == ""
        assert cfg["cwd"] != _LAUNCH_CWD
    assert json.loads(os.environ["TERMINAL_DOCKER_VOLUMES"])  # A unchanged


def test_malformed_profile_config_refuses_execution(tmp_path):
    """Unresolvable policy → refusal scope; terminal_tool refuses instead of
    running under the launch process's ambient policy (fail closed)."""
    from tools.terminal_tool import terminal_tool

    home = _profile(tmp_path, "broken", "terminal: [unclosed\n")
    token = install_profile_terminal_scope(home)
    try:
        assert isinstance(get_terminal_scope(), TerminalPolicyRefusal)
        with pytest.raises(TerminalPolicyUnavailable):
            terminal_env("TERMINAL_ENV")
        result = terminal_tool(command="whoami")
        assert "terminal policy unavailable" in result
    finally:
        reset_terminal_scope(token)


def test_gateway_runtime_scope_resets_on_error(tmp_path):
    import gateway.run as gw

    home = _profile(tmp_path, "qa", "terminal:\n  backend: local\n")
    with pytest.raises(RuntimeError):
        with gw._profile_runtime_scope(home):
            assert terminal_env("TERMINAL_ENV") == "local"
            raise RuntimeError("turn blew up")
    assert get_terminal_scope() is None


def test_tui_and_cron_boundaries_bind_and_reset(tmp_path):
    import tui_gateway.server as server
    from tools.terminal_scope import install_and_reset_profile_terminal_scope

    home = _profile(tmp_path, "dash", "terminal:\n  backend: local\n")
    with server._session_profile_runtime_scope({"profile_home": str(home)}):
        assert terminal_env("TERMINAL_ENV") == "local"
        assert terminal_env("TERMINAL_SSH_HOST") == ""
    assert get_terminal_scope() is None
    with install_and_reset_profile_terminal_scope(home):  # cron fire helper
        assert terminal_env("TERMINAL_ENV") == "local"
    assert get_terminal_scope() is None


def test_config_list_and_dict_values_are_json_not_repr(tmp_path):
    """config.yaml list/dict terminal keys must be JSON so terminal_tool's
    json.loads path succeeds. str() produces Python repr and drops the tool."""
    from tools.terminal_scope import build_profile_terminal_scope

    home = _profile(
        tmp_path,
        "docker-lists",
        "\n".join(
            [
                "terminal:",
                "  backend: docker",
                "  docker_forward_env:",
                "    - EMAIL_HOME_ADDRESS",
                "  docker_volumes:",
                "    - /tmp/a:/data",
                "  docker_env:",
                "    FOO: bar",
                "  docker_extra_args:",
                "    - --network=host",
                "",
            ]
        ),
    )
    scope = build_profile_terminal_scope(home)
    assert json.loads(scope["TERMINAL_DOCKER_FORWARD_ENV"]) == ["EMAIL_HOME_ADDRESS"]
    assert json.loads(scope["TERMINAL_DOCKER_VOLUMES"]) == ["/tmp/a:/data"]
    assert json.loads(scope["TERMINAL_DOCKER_ENV"]) == {"FOO": "bar"}
    assert json.loads(scope["TERMINAL_DOCKER_EXTRA_ARGS"]) == ["--network=host"]

    import gateway.run as gw
    import tools.terminal_tool as tt

    with gw._profile_runtime_scope(home):
        cfg = tt._get_env_config()
        assert cfg["docker_forward_env"] == ["EMAIL_HOME_ADDRESS"]
        assert cfg["docker_volumes"] == ["/tmp/a:/data"]
        assert cfg["docker_env"] == {"FOO": "bar"}
        assert cfg["docker_extra_args"] == ["--network=host"]


def test_dotenv_json_strings_stay_json_strings(tmp_path):
    """The .env path already stores JSON text; str() must keep that payload."""
    from tools.terminal_scope import build_profile_terminal_scope

    home = _profile(
        tmp_path,
        "dotenv-json",
        "terminal:\n  backend: docker\n",
        'TERMINAL_DOCKER_FORWARD_ENV=["EMAIL_HOME_ADDRESS"]\n'
        'TERMINAL_DOCKER_VOLUMES=["/tmp/a:/data"]\n',
    )
    scope = build_profile_terminal_scope(home)
    assert json.loads(scope["TERMINAL_DOCKER_FORWARD_ENV"]) == ["EMAIL_HOME_ADDRESS"]
    assert json.loads(scope["TERMINAL_DOCKER_VOLUMES"]) == ["/tmp/a:/data"]


def _count_config_parses(monkeypatch, delay=0.0):
    """Count fast_safe_load calls made through the module the builder imports from."""
    import hermes_cli.config as config_mod

    loads = []
    real_fast_safe_load = config_mod.fast_safe_load

    def counting_fast_safe_load(stream):
        loads.append(1)
        if delay:
            time.sleep(delay)
        return real_fast_safe_load(stream)

    monkeypatch.setattr(config_mod, "fast_safe_load", counting_fast_safe_load)
    return loads


def test_unchanged_profile_policy_is_parsed_once(tmp_path, monkeypatch):
    """Criterion 1: an unchanged profile is parsed once, whichever surface asks."""
    from tools.terminal_scope import build_profile_terminal_scope

    loads = _count_config_parses(monkeypatch)
    home = _profile(
        tmp_path, "stable",
        "terminal:\n  backend: local\n  docker_image: alpine:3.20\n",
    )

    first = build_profile_terminal_scope(home)
    second = build_profile_terminal_scope(home)

    assert first == second
    assert first["TERMINAL_ENV"] == "local"
    assert first["TERMINAL_DOCKER_IMAGE"] == "alpine:3.20"
    assert len(loads) == 1


def test_cached_policy_is_returned_as_a_defensive_copy(tmp_path, monkeypatch):
    """Criterion 4: mutating a returned mapping must not poison the cache."""
    from tools.terminal_scope import build_profile_terminal_scope

    loads = _count_config_parses(monkeypatch)
    home = _profile(
        tmp_path, "copy",
        "terminal:\n  backend: local\n  docker_image: alpine:3.20\n",
    )

    first = build_profile_terminal_scope(home)
    first["TERMINAL_ENV"] = "docker"
    first["TERMINAL_DOCKER_IMAGE"] = "poisoned"
    second = build_profile_terminal_scope(home)

    assert second is not first
    assert second["TERMINAL_ENV"] == "local"
    assert second["TERMINAL_DOCKER_IMAGE"] == "alpine:3.20"
    assert len(loads) == 1


def test_config_change_invalidates_cached_policy(tmp_path, monkeypatch):
    """Criterion 2 (config.yaml): a mutation is observed on the next call."""
    from tools.terminal_scope import build_profile_terminal_scope

    loads = _count_config_parses(monkeypatch)
    home = _profile(tmp_path, "mut-config", "terminal:\n  backend: local\n")

    assert build_profile_terminal_scope(home)["TERMINAL_ENV"] == "local"

    (home / "config.yaml").write_text(
        "terminal:\n  backend: docker\n  docker_image: alpine:3.20\n", encoding="utf-8"
    )
    scope = build_profile_terminal_scope(home)

    assert scope["TERMINAL_ENV"] == "docker"
    assert scope["TERMINAL_DOCKER_IMAGE"] == "alpine:3.20"
    assert len(loads) == 2


def test_dotenv_change_invalidates_cached_policy(tmp_path):
    """Criterion 2 (.env): a mutation is observed on the next call."""
    from tools.terminal_scope import build_profile_terminal_scope

    home = _profile(
        tmp_path, "mut-dotenv", "",
        "TERMINAL_ENV=local\nTERMINAL_SSH_HOST=10.0.0.1\n",
    )

    assert build_profile_terminal_scope(home)["TERMINAL_ENV"] == "local"

    (home / ".env").write_text(
        "TERMINAL_ENV=docker\nTERMINAL_SSH_HOST=10.0.0.2\n", encoding="utf-8"
    )
    scope = build_profile_terminal_scope(home)

    assert scope["TERMINAL_ENV"] == "docker"
    assert scope["TERMINAL_SSH_HOST"] == "10.0.0.2"


def test_malformed_policy_refusal_is_cached_then_recovers(tmp_path, monkeypatch):
    """Criterion 3: an unchanged malformed file is refused from cache; fixing it recovers."""
    from tools.terminal_scope import build_profile_terminal_scope

    loads = _count_config_parses(monkeypatch)
    home = _profile(tmp_path, "recovers", "terminal: [unclosed\n")

    with pytest.raises(TerminalPolicyUnavailable):
        build_profile_terminal_scope(home)
    with pytest.raises(TerminalPolicyUnavailable):
        build_profile_terminal_scope(home)
    assert len(loads) == 1

    (home / "config.yaml").write_text("terminal:\n  backend: local\n", encoding="utf-8")

    assert build_profile_terminal_scope(home)["TERMINAL_ENV"] == "local"
    assert len(loads) == 2


@pytest.mark.parametrize("config_text", ["[]\n", "terminal: local\n"])
def test_structurally_invalid_config_fails_closed(tmp_path, config_text):
    from tools.terminal_scope import build_profile_terminal_scope

    home = _profile(tmp_path, "invalid-structure", config_text)

    with pytest.raises(TerminalPolicyUnavailable):
        build_profile_terminal_scope(home)


def test_null_terminal_section_preserves_defaults(tmp_path):
    from tools.terminal_scope import build_profile_terminal_scope

    home = _profile(tmp_path, "null-terminal", "terminal: null\n")

    assert build_profile_terminal_scope(home)["TERMINAL_ENV"] == "local"


def test_invalid_utf8_dotenv_fails_closed(tmp_path):
    from tools.terminal_scope import build_profile_terminal_scope

    home = _profile(tmp_path, "invalid-dotenv", "terminal:\n  backend: local\n")
    (home / ".env").write_bytes(b"TERMINAL_ENV=local\xff\n")

    with pytest.raises(TerminalPolicyUnavailable):
        build_profile_terminal_scope(home)


@pytest.mark.parametrize(
    "dotenv_text",
    ["TERMINAL_ENV\n", "TERMINAL ENV=local\n", "TERMINAL_ENV='local\n"],
)
def test_malformed_dotenv_fails_closed(tmp_path, dotenv_text):
    from tools.terminal_scope import build_profile_terminal_scope

    home = _profile(tmp_path, "malformed-dotenv", "terminal:\n  backend: local\n", dotenv_text)

    with pytest.raises(TerminalPolicyUnavailable):
        build_profile_terminal_scope(home)


def test_concurrent_callers_for_one_profile_parse_once(tmp_path, monkeypatch):
    """Criterion 5: concurrent misses single-flight into one parse (criterion 1 across threads)."""
    from concurrent.futures import ThreadPoolExecutor

    from tools.terminal_scope import build_profile_terminal_scope

    loads = _count_config_parses(monkeypatch, delay=0.1)
    home = _profile(tmp_path, "concurrent", "terminal:\n  backend: local\n")
    barrier = threading.Barrier(6)

    def call():
        barrier.wait(timeout=30)
        return build_profile_terminal_scope(home)["TERMINAL_ENV"]

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = [future.result(timeout=60) for future in [pool.submit(call) for _ in range(6)]]

    assert results == ["local"] * 6
    assert len(loads) == 1


def test_torn_read_refuses_and_never_serves_the_stale_cached_mapping(tmp_path, monkeypatch):
    """Criterion 3: a file replaced mid-read refuses once, then parses the stable replacement."""
    import hermes_cli.config as config_mod

    from tools.terminal_scope import build_profile_terminal_scope

    home = _profile(tmp_path, "torn", "terminal:\n  backend: local\n")
    assert build_profile_terminal_scope(home)["TERMINAL_ENV"] == "local"

    real_fast_safe_load = config_mod.fast_safe_load
    parses = []

    def replace_during_parse(stream):
        parses.append(1)
        if len(parses) == 1:
            (home / "config.yaml").write_text("terminal:\n  backend: ssh\n", encoding="utf-8")
        return real_fast_safe_load(stream)

    (home / "config.yaml").write_text("terminal:\n  backend: docker\n", encoding="utf-8")
    monkeypatch.setattr(config_mod, "fast_safe_load", replace_during_parse)

    with pytest.raises(TerminalPolicyUnavailable):
        build_profile_terminal_scope(home)
    assert build_profile_terminal_scope(home)["TERMINAL_ENV"] == "ssh"
    assert len(parses) == 2
