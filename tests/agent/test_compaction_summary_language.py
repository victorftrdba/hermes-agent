"""Regression for #135470: compaction summaries follow the conversation's language.

An English summary is injected as the newest context block and drags a non-English session's
visible reply and reasoning back to English after every compaction. The summarizer prompt now
closes with a directive naming the language its own turns are in (and the configured session
language stands in when the turns carry no script signal).
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from agent.context_compressor import HISTORICAL_TASK_HEADING, ContextCompressor
from agent.context_compressor_prompt import (
    detect_conversation_language,
    summary_language_directive,
    transcript_language_sample,
)

_NO_DIRECTIVE_MARKER = "Write every section below in"


def _response(content: str) -> SimpleNamespace:
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))])


@pytest.fixture()
def compressor() -> ContextCompressor:
    with patch("agent.context_compressor.get_model_context_length", return_value=100_000):
        instance = ContextCompressor(
            model="test/model", threshold_percent=0.50, protect_first_n=0, protect_last_n=2, quiet_mode=True,
        )
    instance.tail_token_budget = 80
    return instance


def _capture_summary_prompt(compressor: ContextCompressor, turns: list[dict]) -> str:
    """The real summarizer prompt for *turns*; only the LLM transport is faked."""
    prompts: list[str] = []

    def _fake_call_llm(**kwargs):
        prompts.append(kwargs["messages"][0]["content"])
        return _response(
            f"{HISTORICAL_TASK_HEADING}\nUser asked for a language check\n\n"
            "## Goal\nKeep the summary in the conversation's language\n"
        )

    with patch("agent.context_compressor.call_llm", side_effect=_fake_call_llm):
        compressor._generate_summary(turns)
    assert prompts, "the summarizer LLM was never called"
    return prompts[0]


def _cjk_turns() -> list[dict]:
    return [
        {"role": "user", "content": "帮我看看为什么每次压缩之后摘要都会变成英文，这很影响使用。"},
        {"role": "assistant", "content": "我先检查一下压缩提示词的拼装逻辑。"},
    ]


def _english_turns() -> list[dict]:
    return [
        {"role": "user", "content": "Please check why the summary keeps coming back in English."},
        {"role": "assistant", "content": "Checking the prompt assembly now."},
    ]


@pytest.mark.parametrize(
    ("sample", "expected"),
    [
        ("请帮我检查一下压缩摘要为什么总是变成英文，谢谢！", "Chinese"),
        ("Пожалуйста, проверь контекст и исправь ошибку в сжатии.", "Russian"),
        ("مرحباً، هل يمكنك مساعدتي في مراجعة هذا الملخص؟", "Arabic"),
        ("שלום, אני צריך עזרה עם סיכום ההקשר הזה.", "Hebrew"),
        ("Καλημέρα, μπορείς να με βοηθήσεις με αυτή τη σύνοψη;", "Greek"),
        ("안녕하세요, 이 요약 문제를 확인해 주세요.", "Korean"),
        ("この要約がなぜ英語になるのか確認してください。", "Japanese"),
        ("Hello world, this is a plain English sentence.", None),
        ("Hello, this is mostly English with exactly one CJK character 你 in it.", None),
        ("", None),
    ],
)
def test_detect_conversation_language(sample, expected):
    assert detect_conversation_language(sample) == expected


def test_directive_names_the_detected_language():
    assert summary_language_directive(
        "请检查一下这个摘要的问题。"
    ) == "Write every section below in Chinese (中文) — the conversation is in Chinese."


def test_language_sample_uses_user_turns_over_assistant_text():
    turns = [
        {"role": "user", "content": "你好，请继续。"},
        {"role": "assistant", "content": "I will continue in English. " * 50},
    ]
    assert transcript_language_sample(turns) == "你好，请继续。"


def test_cjk_conversation_prompt_closes_with_chinese_directive(compressor):
    prompt = _capture_summary_prompt(compressor, _cjk_turns())
    assert prompt.rstrip().endswith(
        "Write every section below in Chinese (中文) — the conversation is in Chinese."
    )


def test_english_conversation_prompt_gets_no_language_directive(compressor, monkeypatch):
    monkeypatch.setattr("agent.i18n.get_language", lambda: "en")
    prompt = _capture_summary_prompt(compressor, _english_turns())
    assert _NO_DIRECTIVE_MARKER not in prompt


def test_tool_output_does_not_decide_the_summary_language(compressor, monkeypatch):
    """Detection reads the conversation turns only: Chinese tool output must not outvote English turns."""
    monkeypatch.setattr("agent.i18n.get_language", lambda: "en")
    turns = [
        {"role": "user", "content": "Please check the decompression warning handling."},
        {"role": "tool", "tool_call_id": "c1", "content": "错误：命令执行失败，退出码 1。" + "错误" * 100},
    ]
    prompt = _capture_summary_prompt(compressor, turns)
    assert _NO_DIRECTIVE_MARKER not in prompt


def test_configured_non_english_language_directs_the_summary(compressor, monkeypatch):
    """Turns with no script signal: the configured session language stands in."""
    monkeypatch.setattr("agent.i18n.get_language", lambda: "zh")
    prompt = _capture_summary_prompt(compressor, _english_turns())
    assert prompt.rstrip().endswith(
        "Write every section below in 简体中文 (zh) — the session language is set to 简体中文."
    )


def test_language_config_failure_degrades_to_todays_prompt(compressor, monkeypatch):
    """A detection/config failure must never break a compaction (#135470)."""

    def _boom() -> str:
        raise RuntimeError("config read failed")

    monkeypatch.setattr("agent.i18n.get_language", _boom)
    assert summary_language_directive("Plain English text.") == ""
    prompt = _capture_summary_prompt(compressor, _english_turns())
    assert _NO_DIRECTIVE_MARKER not in prompt


def test_sample_extraction_failure_degrades_without_breaking_compaction(monkeypatch):
    def unavailable(_content):
        raise RuntimeError("sample unavailable")

    monkeypatch.setattr("agent.context_compressor._content_text_for_contains", unavailable)
    assert transcript_language_sample(_english_turns()) == ""


@pytest.mark.parametrize("previous", [None, "Previous completed checkpoint"])
def test_focus_and_iterative_prompt_keep_the_language_directive_last(compressor, previous):
    compressor._previous_summary = previous
    prompt = compressor._build_summary_prompt("source turns", 200, "language regression", "", True,
                                              "请检查这个摘要的问题。")
    assert 'FOCUS TOPIC: "language regression"' in prompt
    assert prompt.endswith("Write every section below in Chinese (中文) — the conversation is in Chinese.")
