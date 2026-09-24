"""Tests for gateway/shutdown_flush.py — pending message durability (#72680)."""

import itertools
import json
import os
import stat
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from gateway.shutdown_flush import (
    _recover_one_payload,
    _serialise_value,
    drain_transcript_spool,
    flush_overflow_to_file,
    flush_pending_to_file,
    recover_pending_to_db,
    spool_dropped_transcript_message,
)


def _make_flush_dir(tmp_path: Path) -> Path:
    """Create a temp flush dir and monkeypatch _get_flush_dir to use it."""
    flush_dir = tmp_path / "pending_messages"
    flush_dir.mkdir(parents=True, exist_ok=True)
    return flush_dir


def test_flush_writes_string_pending_to_file(tmp_path, monkeypatch):
    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr(
        "gateway.shutdown_flush._get_flush_dir", lambda: flush_dir
    )
    pending = {"agent:main:telegram:supergroup:123": "hello world"}
    count = flush_pending_to_file(pending, reason="shutdown")
    assert count == 1
    files = list(flush_dir.glob("*.json"))
    assert len(files) == 1
    payload = json.loads(files[0].read_text(encoding="utf-8"))
    assert payload["session_key"] == "agent:main:telegram:supergroup:123"
    assert payload["reason"] == "shutdown"
    assert payload["data"]["text"] == "hello world"
    assert ":" not in files[0].name
    assert "telegram" not in files[0].name


def test_flush_writes_message_event_to_file(tmp_path, monkeypatch):
    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr(
        "gateway.shutdown_flush._get_flush_dir", lambda: flush_dir
    )
    event = MagicMock()
    event.text = "user message"
    event.session_id = "20260728_120000_abc"
    event.platform = "telegram"
    event.sender_id = "456"
    event.sender_name = "Alice"
    event.reply_to = None
    event.media = None
    event.raw_event = None

    count = flush_pending_to_file({"session_key_1": event}, reason="adapter_shutdown")
    assert count == 1
    files = list(flush_dir.glob("*.json"))
    assert len(files) == 1
    payload = json.loads(files[0].read_text(encoding="utf-8"))
    assert payload["data"]["text"] == "user message"
    assert payload["data"]["session_id"] == "20260728_120000_abc"


def test_recover_inserts_via_append_message_and_deletes_file(tmp_path, monkeypatch):
    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr(
        "gateway.shutdown_flush._get_flush_dir", lambda: flush_dir
    )
    ts = int(time.time())
    # Write a flush file with session_id
    payload = {
        "session_key": "agent:main:telegram:supergroup:123",
        "reason": "shutdown",
        "ts": ts,
        "data": {
            "text": "lost message",
            "session_id": "20260728_120000_abc",
        },
    }
    flush_file = flush_dir / "test_session_123.json"
    flush_file.write_text(json.dumps(payload), encoding="utf-8")

    mock_db = MagicMock()
    count = recover_pending_to_db(mock_db)

    assert count == 1
    mock_db.append_message.assert_called_once_with(
        session_id="20260728_120000_abc",
        role="user",
        content="lost message",
        timestamp=ts,
    )
    assert not flush_file.exists()


def test_recover_transcript_spool_preserves_metadata_and_deduplicates_owner(
    tmp_path, monkeypatch,
):
    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr("gateway.shutdown_flush._get_flush_dir", lambda: flush_dir)

    class _DB:
        def __init__(self):
            self.calls = []
            self.owners = set()

        def get_compression_tip(self, session_id):
            return session_id

        def has_gateway_input_owner(self, session_id, owner):
            return (session_id, owner) in self.owners

        def get_session(self, session_id):
            return None

        def _is_compression_child_row(self, row):
            return False

        def append_message(self, **kwargs):
            self.calls.append(kwargs)
            owner = (kwargs.get("display_metadata") or {}).get("gateway_input_owner")
            if owner:
                self.owners.add((kwargs["session_id"], owner))

    message = {
        "role": "user",
        "content": "preserve me",
        "message_id": "platform-1",
        "observed": True,
        "timestamp": 123.5,
        "api_content": "exact bytes",
        "display_kind": "internal_notification",
        "display_metadata": {"gateway_input_owner": "owner-1", "user_id": "user-1"},
    }
    assert spool_dropped_transcript_message("session-1", message) is not None
    db = _DB()
    assert recover_pending_to_db(db) == 1
    assert db.calls[0]["platform_message_id"] == "platform-1"
    assert db.calls[0]["observed"] is True
    assert db.calls[0]["api_content"] == "exact bytes"
    assert db.calls[0]["display_kind"] == "internal_notification"
    assert db.calls[0]["display_metadata"] == message["display_metadata"]

    assert spool_dropped_transcript_message("session-1", message) is not None
    assert recover_pending_to_db(db) == 1
    assert len(db.calls) == 1
    assert list(flush_dir.glob("*.json")) == []


def test_recover_transcript_spool_uses_logical_order_not_filename(tmp_path, monkeypatch):
    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr("gateway.shutdown_flush._get_flush_dir", lambda: flush_dir)

    def _payload(seq, content, message_id, metadata):
        return {
            "session_key": "session-1",
            "reason": "transcript_cap_drop",
            "ts": 100,
            "seq": seq,
            "data": {
                "session_id": "session-1",
                "message": {
                    "role": "user",
                    "content": content,
                    "message_id": message_id,
                    "display_metadata": metadata,
                },
            },
        }

    (flush_dir / "a-logical-second.json").write_text(
        json.dumps(_payload(2, "second", "platform-2", {"user_id": "user-2"})),
        encoding="utf-8",
    )
    (flush_dir / "z-logical-first.json").write_text(
        json.dumps(_payload(1, "first", "platform-1", {"user_id": "user-1"})),
        encoding="utf-8",
    )

    db = MagicMock()
    assert recover_pending_to_db(db) == 2
    calls = [call.kwargs for call in db.append_message.call_args_list]
    assert [call["content"] for call in calls] == ["first", "second"]
    assert [call["platform_message_id"] for call in calls] == ["platform-1", "platform-2"]
    assert [call["display_metadata"] for call in calls] == [
        {"user_id": "user-1"},
        {"user_id": "user-2"},
    ]
    assert list(flush_dir.glob("*.json")) == []


def _crash_replay_message(role):
    common = {
        "role": role,
        "content": f"{role} durable content",
        "platform_message_id": f"platform-{role}",
        "observed": True,
        "timestamp": 123.5,
        "api_content": f"{role} exact api content",
        "display_kind": "spool_test",
        "display_metadata": {"marker": role, "nested": {"preserved": True}},
    }
    if role == "assistant":
        common.update({
            "reasoning": "assistant reasoning",
            "reasoning_details": [{"type": "summary", "text": "detail"}],
            "codex_message_items": [{"type": "message", "id": "message-1"}],
        })
    else:
        common.update({"tool_name": "lookup", "tool_call_id": "call-1"})
    return common


def _write_crash_replay_spool(flush_dir, role, *, legacy):
    message = _crash_replay_message(role)
    if not legacy:
        return spool_dropped_transcript_message("session-1", message)
    path = flush_dir / f"pending-legacy-{role}.json"
    path.write_text(json.dumps({
        "session_key": "session-1",
        "reason": "transcript_cap_drop",
        "ts": 100,
        "seq": 1,
        "data": {"session_id": "session-1", "message": message},
    }), encoding="utf-8")
    return path


def _assert_crash_replay_row(row, role):
    assert row["role"] == role
    assert row["content"] == f"{role} durable content"
    assert row["platform_message_id"] == f"platform-{role}"
    assert row["observed"] == 1
    assert row["api_content"] == f"{role} exact api content"
    assert row["display_kind"] == "spool_test"
    assert row["display_metadata"]["marker"] == role
    assert row["display_metadata"]["nested"] == {"preserved": True}
    assert "_hermes_spool_record_id" not in row["display_metadata"]
    if role == "assistant":
        assert row["reasoning"] == "assistant reasoning"
        assert json.loads(row["reasoning_details"]) == [
            {"type": "summary", "text": "detail"},
        ]
        assert json.loads(row["codex_message_items"]) == [
            {"type": "message", "id": "message-1"},
        ]
    else:
        assert row["tool_name"] == "lookup"
        assert row["tool_call_id"] == "call-1"


def _assert_crash_replay_conversation(db, role):
    row = db.get_messages_as_conversation("session-1")[0]
    assert row["display_metadata"] == {
        "marker": role,
        "nested": {"preserved": True},
    }


@pytest.mark.parametrize("role", ["assistant", "tool"])
@pytest.mark.parametrize("legacy", [False, True], ids=["new", "legacy"])
def test_live_drain_is_idempotent_after_commit_before_unlink_crash(
    role, legacy, tmp_path, monkeypatch,
):
    import hermes_state
    from gateway.session import SessionStore

    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr("gateway.shutdown_flush._get_flush_dir", lambda: flush_dir)
    db = hermes_state.SessionDB(db_path=tmp_path / "state.db")
    db.create_session("session-1", source="test")
    store = object.__new__(SessionStore)
    path = _write_crash_replay_spool(flush_dir, role, legacy=legacy)
    assert path is not None

    def _commit_then_crash(message, record_id):
        store._append_transcript_message_to_db(
            db, "session-1", message, spool_record_id=record_id,
        )
        raise RuntimeError("simulated crash after commit")

    assert drain_transcript_spool(
        "session-1", _commit_then_crash, include_record_id=True,
    ) == (0, 1)
    assert path.exists()
    assert len(db.get_messages("session-1")) == 1

    assert drain_transcript_spool(
        "session-1",
        lambda message, record_id: store._append_transcript_message_to_db(
            db, "session-1", message, spool_record_id=record_id,
        ),
        include_record_id=True,
    ) == (1, 0)
    rows = db.get_messages("session-1")
    assert len(rows) == 1
    _assert_crash_replay_row(rows[0], role)
    _assert_crash_replay_conversation(db, role)
    assert not path.exists()
    db.close()


@pytest.mark.parametrize("role", ["assistant", "tool"])
@pytest.mark.parametrize("legacy", [False, True], ids=["new", "legacy"])
def test_restart_recovery_is_idempotent_after_commit_before_unlink_crash(
    role, legacy, tmp_path, monkeypatch,
):
    import hermes_state

    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr("gateway.shutdown_flush._get_flush_dir", lambda: flush_dir)
    db_path = tmp_path / "state.db"
    first_process = hermes_state.SessionDB(db_path=db_path)
    first_process.create_session("session-1", source="test")
    path = _write_crash_replay_spool(flush_dir, role, legacy=legacy)
    assert path is not None
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert _recover_one_payload(first_process, path, payload) is True
    first_process.close()
    assert path.exists()

    restarted_process = hermes_state.SessionDB(db_path=db_path)
    assert recover_pending_to_db(restarted_process) == 1
    rows = restarted_process.get_messages("session-1")
    assert len(rows) == 1
    _assert_crash_replay_row(rows[0], role)
    _assert_crash_replay_conversation(restarted_process, role)
    assert not path.exists()
    restarted_process.close()


def test_spool_identity_is_scoped_to_each_profile_database(tmp_path):
    import hermes_state

    payload = {
        "session_key": "session-1",
        "reason": "transcript_cap_drop",
        "spool_record_id": "shared-record-id",
        "data": {
            "session_id": "session-1",
            "message": {"role": "assistant", "content": "one per profile"},
        },
    }
    for profile in ("profile-a", "profile-b"):
        db = hermes_state.SessionDB(db_path=tmp_path / profile / "state.db")
        db.create_session("session-1", source="test")
        assert _recover_one_payload(db, tmp_path / "spool.json", payload) is True
        assert _recover_one_payload(db, tmp_path / "spool.json", payload) is True
        assert len(db.get_messages("session-1")) == 1
        db.close()


def test_recover_transcript_spool_order_survives_process_restart(tmp_path, monkeypatch):
    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr("gateway.shutdown_flush._get_flush_dir", lambda: flush_dir)
    monkeypatch.setattr("gateway.shutdown_flush.time.time", lambda: 100)
    names = iter(("f" * 32, "0" * 32))
    monkeypatch.setattr(
        "gateway.shutdown_flush.uuid.uuid4",
        lambda: SimpleNamespace(hex=next(names)),
    )

    first = {
        "role": "user",
        "content": "accepted first",
        "message_id": "platform-1",
        "display_metadata": {"user_id": "user-1"},
    }
    second = {
        "role": "user",
        "content": "accepted second",
        "message_id": "platform-2",
        "display_metadata": {"user_id": "user-2"},
    }
    assert spool_dropped_transcript_message("session-1", first) is not None
    monkeypatch.setattr("gateway.shutdown_flush._TRANSCRIPT_SPOOL_SEQ", itertools.count())
    assert spool_dropped_transcript_message("session-1", second) is not None

    db = MagicMock()
    assert recover_pending_to_db(db) == 2
    calls = [call.kwargs for call in db.append_message.call_args_list]
    assert [call["content"] for call in calls] == ["accepted first", "accepted second"]
    assert [call["platform_message_id"] for call in calls] == ["platform-1", "platform-2"]
    assert [call["display_metadata"] for call in calls] == [
        {"user_id": "user-1"},
        {"user_id": "user-2"},
    ]


def test_recover_closes_owned_db_when_unexpected_exception_escapes(
    tmp_path, monkeypatch
):
    """Owned SessionDB must close even when recovery is interrupted."""
    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr(
        "gateway.shutdown_flush._get_flush_dir", lambda: flush_dir
    )
    (flush_dir / "pending.json").write_text(
        json.dumps(
            {
                "session_key": "agent:main:telegram:123",
                "data": {"text": "message", "session_id": "sid"},
            }
        ),
        encoding="utf-8",
    )

    class InterruptingDB:
        released = False

        def append_message(self, **_kwargs):
            raise KeyboardInterrupt

    db = InterruptingDB()
    monkeypatch.setattr("hermes_state_registry.acquire", lambda: db)
    monkeypatch.setattr(
        "hermes_state_registry.release_or_close", lambda _: setattr(db, "released", True)
    )

    with pytest.raises(KeyboardInterrupt):
        recover_pending_to_db()

    assert db.released is True


def test_serialise_object_with_text():
    obj = MagicMock()
    obj.text = "msg"
    obj.session_id = "sid"
    obj.platform = None
    obj.sender_id = None
    obj.sender_name = None
    obj.reply_to = None
    obj.media = None
    obj.raw_event = None
    result = _serialise_value(obj)
    assert result is not None
    assert result["text"] == "msg"
    assert result["session_id"] == "sid"


def test_get_flush_dir_uses_get_hermes_home(tmp_path, monkeypatch):
    """Flush dir must use get_hermes_home(), not hardcoded Path.home()."""
    import gateway.shutdown_flush as mod

    captured = {}

    def fake_get_hermes_home():
        from pathlib import Path
        captured["called"] = True
        return tmp_path

    monkeypatch.setattr(
        "hermes_constants.get_hermes_home", fake_get_hermes_home
    )
    result = mod._get_flush_dir()
    assert captured.get("called") is True
    assert result == tmp_path / "pending_messages"




# ── FIFO overflow tail durability (#99882) ─────────────────────────────


def _overflow_event(text: str, session_id: str = "20260901_120000_fifo"):
    event = MagicMock()
    event.text = text
    event.session_id = session_id
    event.platform = "telegram"
    event.sender_id = "1572286605"
    event.sender_name = "tester"
    event.reply_to = None
    event.media = None
    event.raw_event = None
    return event


def test_flush_overflow_writes_one_payload_per_event_in_arrival_order(tmp_path, monkeypatch):
    """The FIFO tail (queued_events) must survive shutdown like the slot does.

    Each overflow entry is its own recover_pending_to_db-compatible payload,
    with ``seq`` recording arrival order inside the session.
    """
    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr("gateway.shutdown_flush._get_flush_dir", lambda: flush_dir)

    count = flush_overflow_to_file(
        {
            "agent:main:telegram:dm:1": [
                _overflow_event("follow-up B"),
                _overflow_event("follow-up C"),
            ],
            "agent:main:telegram:dm:2": [],
            "": [_overflow_event("keyless — skipped")],
        },
        reason="shutdown",
    )
    assert count == 2
    payloads = sorted(
        (json.loads(f.read_text(encoding="utf-8")) for f in flush_dir.glob("*.json")),
        key=lambda p: p["seq"],
    )
    assert [p["data"]["text"] for p in payloads] == ["follow-up B", "follow-up C"]
    assert {p["session_key"] for p in payloads} == {"agent:main:telegram:dm:1"}
    assert all(p["reason"] == "shutdown" for p in payloads)


def test_flushed_overflow_is_replayed_by_recover_pending_to_db(tmp_path, monkeypatch):
    """Round-trip: overflow payloads use the slot-flush shape, so the existing
    startup recovery inserts them as user rows without any new reader."""
    flush_dir = _make_flush_dir(tmp_path)
    monkeypatch.setattr("gateway.shutdown_flush._get_flush_dir", lambda: flush_dir)
    flush_overflow_to_file({"agent:main:telegram:dm:1": [_overflow_event("orphan-1")]})

    db = MagicMock()
    recovered = recover_pending_to_db(session_db=db)
    assert recovered == 1
    db.append_message.assert_called_once()
    kwargs = db.append_message.call_args.kwargs
    assert kwargs["session_id"] == "20260901_120000_fifo"
    assert kwargs["role"] == "user"
    assert kwargs["content"] == "orphan-1"
    assert list(flush_dir.glob("*.json")) == []


def test_flush_overflow_noop_on_empty():
    assert flush_overflow_to_file({}) == 0
    assert flush_overflow_to_file({"k": []}) == 0
