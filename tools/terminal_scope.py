"""Per-turn terminal scope: profile-scoped TERMINAL_* policy.

Multiplexed surfaces (gateway, dashboard/TUI, cron) serve several profiles from one process;
mirroring terminal settings into ``os.environ`` let the first profile pin its backend onto
everyone else (sandbox escape). Like ``agent/secret_scope.py``, a ContextVar holds the active
profile's COMPLETE ``TERMINAL_*`` policy; while bound, ``terminal_env`` resolves ONLY from it
(omitted keys -> defined default, never ambient env). If the policy cannot be resolved a
*refusal* scope is installed and terminal execution raises :class:`TerminalPolicyUnavailable`.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple

logger = logging.getLogger(__name__)

# None = no scope bound (process-env behavior); dict = complete policy; Refusal = resolution failed.
_terminal_scope_var: ContextVar = ContextVar("hermes_terminal_scope", default=None)

# Keys whose default lives in terminal_tool.py, not DEFAULT_CONFIG (which wins on overlap);
# without them the projection is not total.
_TOOL_LEVEL_DEFAULTS: Dict[str, Any] = {
    "cwd": ".", "ssh_host": "", "ssh_user": "", "ssh_port": 22, "ssh_key": "",
    "docker_orphan_reaper": True, "docker_persist_across_processes": True,
    "sandbox_dir": "", "lifetime_seconds": 300, "docker_shared_container_key": "",
    "home_mode": "auto",
}


class TerminalPolicyUnavailable(Exception):
    """The routed profile's ``.env``/``config.yaml`` exists but cannot be read/parsed."""


class TerminalPolicyRefusal(Dict[str, str]):
    """Marker scope (empty dict subclass) installed when policy resolution failed."""

    def __init__(self, reason: str) -> None:
        super().__init__()
        self.reason = reason


def set_terminal_scope(mapping: Optional[Dict[str, str]]) -> Token:
    """Install *mapping* as the current context's terminal policy."""
    return _terminal_scope_var.set(mapping)


def reset_terminal_scope(token: Token) -> None:
    _terminal_scope_var.reset(token)


def get_terminal_scope() -> Optional[Dict[str, str]]:
    """The active scope mapping/refusal, or ``None`` when no scope is bound."""
    return _terminal_scope_var.get()


def enforce_no_refusal() -> None:
    """Raise when the active scope is a refusal scope (fail closed).

    Execution paths (terminal tool, execute_code) call this before spawning anything: under a refusal scope
    the profile's terminal policy could not be resolved, and running with the launch process's ambient
    policy is exactly the authority leak this module closes (#68559 requires refusal, not fallback).
    Non-scoped and policy-scoped contexts pass silently.
    """
    scope = _terminal_scope_var.get()
    if isinstance(scope, TerminalPolicyRefusal):
        raise TerminalPolicyUnavailable(
            f"terminal policy unavailable for this profile: {scope.reason}")


def terminal_env(name: str, default: str = "") -> str:
    """Authoritative read of a ``TERMINAL_*`` variable.

    No scope: process env, then *default*. Refusal scope: raise. Policy scope: ONLY the
    policy; a missing key yields *default*, never os.environ.
    """
    scope = _terminal_scope_var.get()
    if scope is None:
        return os.environ.get(name, default)
    enforce_no_refusal()
    value = scope.get(name)
    return default if value is None else str(value)


_FileSignature = Optional[Tuple[int, int, int, int, int]]
_ScopeFingerprint = Tuple[_FileSignature, _FileSignature]


@dataclass(frozen=True)
class _CachedScope:
    """A policy outcome pinned to the exact fingerprint of the files it was read from."""

    fingerprint: _ScopeFingerprint
    scope: Optional[Dict[str, str]] = None
    error: Optional[str] = None

    def resolve(self) -> Dict[str, str]:
        if self.error is not None:
            raise TerminalPolicyUnavailable(self.error)
        return dict(self.scope or {})


# Canonical home -> latest outcome and -> single-flight parse lock; both guarded by _SCOPE_CACHE_LOCK.
_SCOPE_CACHE: Dict[str, _CachedScope] = {}
_SCOPE_CACHE_LOCK = threading.Lock()
_HOME_LOCKS: Dict[str, threading.Lock] = {}


def _home_lock(home_key: str) -> threading.Lock:
    with _SCOPE_CACHE_LOCK:
        lock = _HOME_LOCKS.get(home_key)
        if lock is None:
            lock = threading.Lock()
            _HOME_LOCKS[home_key] = lock
        return lock


def _file_signature(path: Path) -> _FileSignature:
    """Missing file -> None; present -> identity, size and mtime/ctime nanoseconds."""
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise TerminalPolicyUnavailable(f"cannot stat {path}: {exc}") from exc
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def _scope_fingerprint(home: Path) -> _ScopeFingerprint:
    return (_file_signature(home / ".env"), _file_signature(home / "config.yaml"))


def _validate_dotenv(text: str, path: Path) -> None:
    for line_number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, separator, value = line.partition("=")
        if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key.strip()):
            raise TerminalPolicyUnavailable(f"cannot parse {path}: invalid line {line_number}")
        value = value.strip()
        if value.startswith(("'", '"')):
            quote = value[0]
            escaped = False
            closing_index = None
            for index, character in enumerate(value[1:], start=1):
                if quote == '"' and character == "\\" and not escaped:
                    escaped = True
                    continue
                if character == quote and not escaped:
                    closing_index = index
                    break
                escaped = False
            if closing_index is None:
                raise TerminalPolicyUnavailable(
                    f"cannot parse {path}: unterminated quote on line {line_number}"
                )
            trailing = value[closing_index + 1:].lstrip()
            if trailing and not trailing.startswith("#"):
                raise TerminalPolicyUnavailable(f"cannot parse {path}: invalid line {line_number}")


def build_profile_terminal_scope(hermes_home: "Any") -> Dict[str, str]:
    """Build the COMPLETE effective ``TERMINAL_*`` policy for a profile home.

    Projection: ``DEFAULT_CONFIG['terminal']`` <- profile ``.env`` TERMINAL_* <- profile
    ``config.yaml`` ``terminal:``. Total by construction, so a bound scope never widens back to
    ambient authority. Raises :class:`TerminalPolicyUnavailable` if a present file is unreadable.

    Outcomes are cached per canonical home under a single-flight lock, keyed by the
    ``(st_dev, st_ino, st_size, st_mtime_ns, st_ctime_ns)`` fingerprint of BOTH files; an exact
    hit returns a defensive copy. Replacing either file changes its fingerprint and forces a
    re-parse. The fingerprint is checked again after reading: a file replaced mid-read yields a
    refusal rather than a torn mapping, and the next call parses the replacement. Parse failures
    are cached against their unchanged fingerprint, so a broken file is not re-parsed and fixing
    it recovers.
    """
    home = Path(hermes_home).resolve()
    home_key = str(home)
    with _home_lock(home_key):
        before = _scope_fingerprint(home)
        with _SCOPE_CACHE_LOCK:
            cached = _SCOPE_CACHE.get(home_key)
            if cached is not None and cached.fingerprint == before:
                return cached.resolve()
        error: Optional[str] = None
        try:
            scope: Optional[Dict[str, str]] = _build_profile_terminal_scope_uncached(home)
        except TerminalPolicyUnavailable as exc:
            scope = None
            error = str(exc)
        after = _scope_fingerprint(home)
        if after != before:
            scope = None
            error = f"terminal policy files under {home} changed while being read"
        entry = _CachedScope(
            fingerprint=before if after != before else after,
            scope=scope,
            error=error,
        )
        with _SCOPE_CACHE_LOCK:
            _SCOPE_CACHE[home_key] = entry
        return entry.resolve()


def _build_profile_terminal_scope_uncached(home: Path) -> Dict[str, str]:
    """Parse a profile's files into its policy; no caching, one full read."""
    from hermes_cli.config import TERMINAL_CONFIG_ENV_MAP, _terminal_env_value
    from hermes_cli.config_defaults import DEFAULT_CONFIG

    scope: Dict[str, str] = {}

    def _apply(mapping: Dict[str, Any]) -> None:
        for cfg_key, value in mapping.items():
            # cwd placeholders are resolved per-surface later; not a policy value.
            if value is None or (cfg_key == "cwd" and str(value).strip() in {".", "auto", "cwd"}):
                continue
            env_var = TERMINAL_CONFIG_ENV_MAP.get(cfg_key)
            if env_var:
                # List/dict config values must be JSON (same contract as
                # apply_terminal_config_to_env). str() yields Python repr, which
                # json.loads in terminal_tool rejects.
                scope[env_var] = _terminal_env_value(value)

    _apply({**_TOOL_LEVEL_DEFAULTS, **(DEFAULT_CONFIG.get("terminal") or {})})
    env_path = home / ".env"
    if env_path.exists():
        # load_env_file swallows OSError by design (secret scope fails soft); an unreadable
        # profile .env must fail closed here.
        try:
            env_text = env_path.read_text(encoding="utf-8-sig")
        except Exception as exc:
            raise TerminalPolicyUnavailable(f"cannot read {env_path}: {exc}") from exc
        _validate_dotenv(env_text, env_path)
        from agent.secret_scope import load_env_file

        scope.update((k, str(v)) for k, v in load_env_file(env_path).items()
                     if k.startswith("TERMINAL_"))
    # Read config.yaml directly, not via read_raw_config() (which collapses "missing" and
    # "unparseable" into {}): present-but-unparseable must fail closed.
    config_path = home / "config.yaml"
    try:
        config_exists = config_path.exists()
    except Exception as exc:
        raise TerminalPolicyUnavailable(f"cannot resolve terminal config in {home}: {exc}") from exc
    if config_exists:
        from hermes_cli.config import fast_safe_load

        try:
            with open(config_path, encoding="utf-8") as f:
                raw = fast_safe_load(f)
        except Exception as exc:
            raise TerminalPolicyUnavailable(f"cannot parse {config_path}: {exc}") from exc
        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            raise TerminalPolicyUnavailable(f"cannot parse {config_path}: root must be a mapping")
        raw_terminal = raw.get("terminal")
        if raw_terminal is not None and not isinstance(raw_terminal, dict):
            raise TerminalPolicyUnavailable(
                f"cannot parse {config_path}: terminal must be a mapping"
            )
        if raw_terminal is not None:
            _apply(raw_terminal)
    return scope


def install_profile_terminal_scope(hermes_home: "Any") -> Token:
    """Build AND install a profile's policy; on failure install the refusal scope. Never raises."""
    try:
        return set_terminal_scope(build_profile_terminal_scope(hermes_home))
    except TerminalPolicyUnavailable as exc:
        logger.warning("terminal policy unavailable: %s", exc)
        return _terminal_scope_var.set(TerminalPolicyRefusal(str(exc)))


@contextmanager
def install_and_reset_profile_terminal_scope(hermes_home: "Any") -> Iterator[None]:
    """Install the profile's terminal policy for a bounded turn/fire. Never raises."""
    token = install_profile_terminal_scope(hermes_home)
    try:
        yield
    finally:
        reset_terminal_scope(token)


# ---- BEGIN PLUGIN-COMPAT (revert-scheduled; see COMPAT_MANIFEST.md) ----
# Names external plugins imported from this module before the Sep 2026 decomposition.
# Internal code MUST NOT use these (scripts/check_compat_pointers.py fails CI if it does).
# The whole block is removed by reverting the commit that added it.

def install_refusal_scope(reason: str) -> Token:
    """Install a refusal scope after :class:`TerminalPolicyUnavailable`.

    Terminal execution under this scope is rejected (fail closed) instead of
    running under the launch process's ambient policy.
    """
    return _terminal_scope_var.set(TerminalPolicyRefusal(reason))

@contextmanager
def terminal_scope(mapping: Optional[Dict[str, str]]) -> Iterator[None]:
    """Context manager form of set/reset_terminal_scope."""
    token = set_terminal_scope(mapping)
    try:
        yield
    finally:
        reset_terminal_scope(token)
# ---- END PLUGIN-COMPAT ----
