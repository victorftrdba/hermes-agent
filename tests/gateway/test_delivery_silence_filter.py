"""Tests for the outbound silence-narration filter (anti-loop control).

See the gateway delivery path: hallucinated "silence" tokens like ``*(silent)*``
are dropped pre-send so bot-to-bot channels can't mirror them into a token-burning
loop that crashes a model with "no content after all retries".

Regression for #135444: the vocabulary and the length guard are declared config data
(``silence_narration_phrases`` / ``silence_narration_max_length``) so an instance speaking
another language can drop its own narrations; an absent declaration reproduces the shipped
English behaviour exactly.
"""

import pytest

from gateway.config import GatewayConfig, Platform, load_gateway_config
from gateway.delivery import (
    DeliveryRouter,
    DeliveryTarget,
    _is_silence_narration,
)


# --- Truth table -----------------------------------------------------------

POSITIVE_CASES = [
    "*(silent)*",
    "*Silence.*",
    "🔇",
    ".",
    "…",
    "...",
    "(silent)",
    "_silent_",
    "silent",
    " *(silent)* ",
    "`silent`",
    "~silent~",
    "Silence",
    "no response",
    "No Reply.",
]

NEGATIVE_CASES = [
    "Silence is golden — here is the plan...",
    "Silent install completed",
    "The deployment ran silently in the background",
    "ok",
    "👍",
    "Here is the result:\n\n- item one\n- item two",
    "I have nothing to add, but here is why: the build is green.",
    "silently",  # word boundary — trailing letters mean it isn't a bare token
    "no responses were collected from the survey",
    # A 64+ char string that opens with a silence token must not be dropped.
    "silent " + "x" * 70,
    # A substantive message that merely CONTAINS a silence word (the reporter's positive control).
    "salut, dar vorbesc despre silence narration aici",
    "",
    "   ",
]


@pytest.mark.parametrize("content", POSITIVE_CASES)
def test_is_silence_narration_positive(content):
    assert _is_silence_narration(content) is True


@pytest.mark.parametrize("content", NEGATIVE_CASES)
def test_real_replies_are_not_silence_narration(content):
    assert _is_silence_narration(content) is False


def test_length_guard_rejects_long_strings():
    # Exactly 65 chars of dots — over the 64-char guard, so not treated as narration.
    assert _is_silence_narration("." * 65) is False
    assert _is_silence_narration("." * 64) is True


# --- Integration through DeliveryRouter ------------------------------------

class RecordingAdapter:
    def __init__(self):
        self.calls = []

    async def send(self, chat_id, content, metadata=None):
        self.calls.append({"chat_id": chat_id, "content": content, "metadata": metadata})
        return {"success": True}


@pytest.mark.asyncio
async def test_silence_narration_dropped_pre_send(tmp_path, monkeypatch):
    monkeypatch.setattr("gateway.delivery.get_hermes_home", lambda: tmp_path)
    adapter = RecordingAdapter()
    router = DeliveryRouter(GatewayConfig(), adapters={Platform.DISCORD: adapter})
    target = DeliveryTarget.parse("discord:99887766")

    result = await router._deliver_to_platform(target, "*(silent)*", metadata=None)

    assert adapter.calls == []  # adapter.send never invoked
    assert result == {
        "success": True,
        "filtered": "silence_narration",
        "delivered": False,
    }


@pytest.mark.asyncio
async def test_config_opt_out_lets_silence_through(tmp_path, monkeypatch):
    monkeypatch.setattr("gateway.delivery.get_hermes_home", lambda: tmp_path)
    adapter = RecordingAdapter()
    config = GatewayConfig(filter_silence_narration=False)
    router = DeliveryRouter(config, adapters={Platform.DISCORD: adapter})
    target = DeliveryTarget.parse("discord:99887766")

    result = await router._deliver_to_platform(target, "*(silent)*", metadata=None)

    assert len(adapter.calls) == 1
    assert adapter.calls[0]["content"] == "*(silent)*"
    assert result == {"success": True}


@pytest.mark.asyncio
async def test_env_does_not_override_config_opt_out(tmp_path, monkeypatch):
    # env shouldn't override config when filter is turned off
    monkeypatch.setattr("gateway.delivery.get_hermes_home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_FILTER_SILENCE_NARRATION", "1")
    adapter = RecordingAdapter()
    config = GatewayConfig(filter_silence_narration=False)
    router = DeliveryRouter(config, adapters={Platform.DISCORD: adapter})
    target = DeliveryTarget.parse("discord:99887766")

    result = await router._deliver_to_platform(target, "*(silent)*", metadata=None)

    assert len(adapter.calls) == 1
    assert adapter.calls[0]["content"] == "*(silent)*"
    assert result == {"success": True}


@pytest.mark.asyncio
async def test_env_does_not_disable_filter_when_config_enabled(tmp_path, monkeypatch):
    # env shouldn't disable filter when config has it on
    monkeypatch.setattr("gateway.delivery.get_hermes_home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_FILTER_SILENCE_NARRATION", "0")
    adapter = RecordingAdapter()
    config = GatewayConfig(filter_silence_narration=True)
    router = DeliveryRouter(config, adapters={Platform.DISCORD: adapter})
    target = DeliveryTarget.parse("discord:99887766")

    result = await router._deliver_to_platform(target, "*(silent)*", metadata=None)

    assert adapter.calls == []
    assert result == {
        "success": True,
        "filtered": "silence_narration",
        "delivered": False,
    }


@pytest.mark.asyncio
async def test_multiplex_profiles_independent_silence_narration_filtering(tmp_path, monkeypatch):
    # secondary profile keeps its own setting under multiplex regardless of process env
    monkeypatch.setattr("gateway.delivery.get_hermes_home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_FILTER_SILENCE_NARRATION", "1")
    adapter_a = RecordingAdapter()
    adapter_b = RecordingAdapter()
    router_a = DeliveryRouter(GatewayConfig(filter_silence_narration=True), adapters={Platform.DISCORD: adapter_a})
    router_b = DeliveryRouter(GatewayConfig(filter_silence_narration=False), adapters={Platform.DISCORD: adapter_b})
    target = DeliveryTarget.parse("discord:99887766")

    res_a = await router_a._deliver_to_platform(target, "*(silent)*", metadata=None)
    res_b = await router_b._deliver_to_platform(target, "*(silent)*", metadata=None)

    assert adapter_a.calls == []
    assert res_a["filtered"] == "silence_narration"
    assert len(adapter_b.calls) == 1
    assert adapter_b.calls[0]["content"] == "*(silent)*"
    assert res_b == {"success": True}


# --- Cron artifacts are exempt ----------------------------------------------
#
# The filter exists to stop bot-to-bot mirror loops of *model chatter*. Cron
# output is an artifact: a job that legitimately emits "..." (a quiet script,
# a terse digest) has no loop partner, and dropping it while returning
# {"success": True} produced a cron the scheduler logged as delivered and the
# user never received (#77763). Cron sends carry job_id in metadata.


@pytest.mark.asyncio
async def test_cron_job_id_metadata_bypasses_the_filter(tmp_path, monkeypatch):
    monkeypatch.setattr("gateway.delivery.get_hermes_home", lambda: tmp_path)
    adapter = RecordingAdapter()
    router = DeliveryRouter(GatewayConfig(), adapters={Platform.DISCORD: adapter})
    target = DeliveryTarget.parse("discord:99887766")

    result = await router._deliver_to_platform(
        target, "*(silent)*", metadata={"job_id": "92e639af907f"},
    )

    assert len(adapter.calls) == 1
    assert adapter.calls[0]["content"] == "*(silent)*"
    assert result.get("filtered") is None
    assert result.get("delivered") is not False


@pytest.mark.asyncio
async def test_non_cron_metadata_still_filters(tmp_path, monkeypatch):
    """The exemption keys on job_id alone — everything else is unchanged."""
    monkeypatch.setattr("gateway.delivery.get_hermes_home", lambda: tmp_path)
    adapter = RecordingAdapter()
    router = DeliveryRouter(GatewayConfig(), adapters={Platform.DISCORD: adapter})
    target = DeliveryTarget.parse("discord:99887766")

    result = await router._deliver_to_platform(
        target, "*(silent)*", metadata={"thread_id": "42", "user_id": "u1"},
    )

    assert adapter.calls == []
    assert result["filtered"] == "silence_narration"


# --- Declared vocabulary and length guard (#135444) --------------------------
#
# The narration vocabulary and the length guard are declared config data
# (``silence_narration_phrases`` / ``silence_narration_max_length``): a non-English instance
# drops its own narrations the way the shipped English set is dropped, a message that merely
# *contains* a declared phrase is still delivered (the anchoring is load-bearing), and an
# absent or unusable declaration reproduces the shipped behaviour exactly.

# The six Romanian narrations the reporter measured being delivered verbatim, and the phrases
# an instance declares to drop them (measured chars: 10 / 32 / 33 / 36 / 82 / 131).
RO_MEASURED_NARRATIONS = [
    "_(tăcere)_",
    "_(fără livrare: no_answer_owed)_",
    "_(fără livrare — no_answer_owed)_",
    "_(tăcere — clasa nu poartă răspuns)_",
    "_(tăcere — no_answer_owed: clasa nu poartă text și nu se livrează niciun răspuns)_",
    "_(fără răspuns: verdictul no_answer_owed nu poartă text — clasa nu compune niciun răspuns, "
    "iar pe linia asta nu se livrează nimic)_",
]
RO_DECLARED_PHRASES = [
    "tăcere",
    "fără livrare: no_answer_owed",
    "fără livrare — no_answer_owed",
    "tăcere — clasa nu poartă răspuns",
    "tăcere — no_answer_owed: clasa nu poartă text și nu se livrează niciun răspuns",
    "fără răspuns: verdictul no_answer_owed nu poartă text — clasa nu compune niciun răspuns, "
    "iar pe linia asta nu se livrează nimic",
]
# The reporter's longest narration is 131 chars; the shipped guard is 64, so the declaration
# has to carry both the vocabulary and the raised limit.
RO_DECLARED_MAX_LENGTH = 140


@pytest.mark.parametrize("content", ["_(silent)_", "_silence_", "🔇", ".", "…"])
def test_empty_declaration_reproduces_the_shipped_english_filter(content):
    assert _is_silence_narration(content) is True
    assert _is_silence_narration(content, phrases=[]) is True


@pytest.mark.parametrize("narration", RO_MEASURED_NARRATIONS)
def test_declared_romanian_vocabulary_drops_each_measured_narration(narration):
    assert _is_silence_narration(
        narration, phrases=RO_DECLARED_PHRASES, max_length=RO_DECLARED_MAX_LENGTH) is True


@pytest.mark.parametrize("content", [
    "salut, dar vorbesc despre silence narration aici",
    "nu vreau tăcere acum — vreau răspunsul complet",
])
def test_declared_vocabulary_never_drops_a_message_that_merely_contains_a_phrase(content):
    assert _is_silence_narration(
        content, phrases=RO_DECLARED_PHRASES, max_length=RO_DECLARED_MAX_LENGTH) is False


def test_declared_vocabulary_needs_the_declared_limit_for_the_long_narrations():
    # The length guard is declared beside the vocabulary: the longest narration is delivered
    # under the shipped 64-char guard even with its phrase declared, dropped under the raised
    # limit, and delivered again past that limit.
    longest = RO_MEASURED_NARRATIONS[-1]
    assert _is_silence_narration(longest, phrases=RO_DECLARED_PHRASES) is False
    assert _is_silence_narration(
        longest, phrases=RO_DECLARED_PHRASES, max_length=RO_DECLARED_MAX_LENGTH) is True
    assert len(longest) <= RO_DECLARED_MAX_LENGTH
    over_limit = longest + " dar mesajul continuă aici"
    assert len(over_limit) > RO_DECLARED_MAX_LENGTH
    assert _is_silence_narration(
        over_limit, phrases=RO_DECLARED_PHRASES, max_length=RO_DECLARED_MAX_LENGTH) is False


def test_declared_length_guard_moves_the_boundary():
    # 32 and 33 chars, both fully declared phrases: narrowing the guard to 32 delivers the
    # 33-char narration while the 32-char one stays dropped.
    just_under = "_(fără livrare: no_answer_owed)_"
    just_over = "_(fără livrare — no_answer_owed)_"
    assert len(just_under) == 32 and len(just_over) == 33
    assert _is_silence_narration(just_under, phrases=RO_DECLARED_PHRASES) is True
    assert _is_silence_narration(just_over, phrases=RO_DECLARED_PHRASES) is True
    assert _is_silence_narration(just_under, phrases=RO_DECLARED_PHRASES, max_length=32) is True
    assert _is_silence_narration(just_over, phrases=RO_DECLARED_PHRASES, max_length=32) is False


def test_unusable_declaration_degrades_to_the_shipped_filter():
    # A broken declaration is ignored, never raised: unusable phrase entries are skipped, a
    # wrong container falls back to the built-ins, and a garbage guard keeps the 64-char default.
    assert _is_silence_narration("_(tăcere)_", phrases=["", None, 7, "tăcere"]) is True
    assert _is_silence_narration("nu vreau tăcere acum", phrases=["", None, 7, "tăcere"]) is False
    assert _is_silence_narration("_(silent)_", phrases="tăcere", max_length="wide") is True
    assert _is_silence_narration("." * 64, max_length=-5) is True
    assert _is_silence_narration("." * 65, max_length=-5) is False


@pytest.mark.asyncio
async def test_declared_vocabulary_drops_narrations_through_the_delivery_path(tmp_path, monkeypatch):
    monkeypatch.setattr("gateway.delivery.get_hermes_home", lambda: tmp_path)
    adapter = RecordingAdapter()
    router = DeliveryRouter(
        GatewayConfig(
            silence_narration_phrases=RO_DECLARED_PHRASES,
            silence_narration_max_length=RO_DECLARED_MAX_LENGTH,
        ),
        adapters={Platform.DISCORD: adapter},
    )
    target = DeliveryTarget.parse("discord:99887766")

    for narration in RO_MEASURED_NARRATIONS:
        result = await router._deliver_to_platform(target, narration, metadata=None)
        assert result["filtered"] == "silence_narration", narration
        assert result["delivered"] is False, narration
    assert adapter.calls == []


@pytest.mark.asyncio
async def test_declared_limit_delivers_an_over_limit_narration(tmp_path, monkeypatch):
    monkeypatch.setattr("gateway.delivery.get_hermes_home", lambda: tmp_path)
    adapter = RecordingAdapter()
    router = DeliveryRouter(
        GatewayConfig(
            silence_narration_phrases=RO_DECLARED_PHRASES,
            silence_narration_max_length=RO_DECLARED_MAX_LENGTH,
        ),
        adapters={Platform.DISCORD: adapter},
    )
    target = DeliveryTarget.parse("discord:99887766")
    content = RO_MEASURED_NARRATIONS[-1] + " dar mesajul continuă aici"

    result = await router._deliver_to_platform(target, content, metadata=None)

    assert len(adapter.calls) == 1
    assert adapter.calls[0]["content"] == content
    assert result == {"success": True}


@pytest.mark.asyncio
async def test_undeclared_instance_keeps_delivering_foreign_narrations(tmp_path, monkeypatch):
    # The filter gains a declaration, not a language detector: an install that declares
    # nothing (every existing install) keeps delivering narrations it never described.
    monkeypatch.setattr("gateway.delivery.get_hermes_home", lambda: tmp_path)
    adapter = RecordingAdapter()
    router = DeliveryRouter(GatewayConfig(), adapters={Platform.DISCORD: adapter})
    target = DeliveryTarget.parse("discord:99887766")

    result = await router._deliver_to_platform(
        target, RO_MEASURED_NARRATIONS[0], metadata=None)

    assert len(adapter.calls) == 1
    assert adapter.calls[0]["content"] == RO_MEASURED_NARRATIONS[0]
    assert result == {"success": True}


# --- Config round-trip ------------------------------------------------------


def test_declared_config_reads_phrases_and_guard_from_config_data():
    config = GatewayConfig.from_dict({
        "silence_narration_phrases": ["tăcere", "", None, 42, "  fără livrare: no_answer_owed  "],
        "silence_narration_max_length": "140",
    })

    assert config.silence_narration_phrases == ["tăcere", "fără livrare: no_answer_owed"]
    assert config.silence_narration_max_length == 140


def test_unusable_declared_config_falls_back_to_the_shipped_defaults():
    config = GatewayConfig.from_dict({
        "silence_narration_phrases": "tăcere",  # a bare string, not a list
        "silence_narration_max_length": "not-a-number",
    })

    assert config.silence_narration_phrases == []
    assert config.silence_narration_max_length == 64


def test_config_yaml_declaration_reaches_the_gateway_config(tmp_path, monkeypatch):
    """The operator-facing seam: a real config.yaml declaration lands in GatewayConfig."""
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "silence_narration_phrases:\n"
        "  - tăcere\n"
        '  - "fără livrare: no_answer_owed"\n'
        "silence_narration_max_length: 140\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))

    config = load_gateway_config()

    assert config.silence_narration_phrases == ["tăcere", "fără livrare: no_answer_owed"]
    assert config.silence_narration_max_length == 140


def test_declared_phrases_are_literal_not_regex():
    phrase = "a.b|c (check)?"
    assert _is_silence_narration(phrase, phrases=[phrase])
    assert not _is_silence_narration("axb|c check", phrases=[phrase])
    assert not _is_silence_narration("prefix " + phrase, phrases=[phrase])


def test_raw_and_loaded_length_declarations_have_the_same_contract():
    config = GatewayConfig.from_dict({"silence_narration_max_length": "140"})
    assert _is_silence_narration("." * 70, max_length="140")
    assert _is_silence_narration("." * 70, max_length=config.silence_narration_max_length)


