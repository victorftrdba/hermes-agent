"""Regression tests for the _find_all_skills discovery cache (#58985 salvage).

Covers the cache-signature fix layered on the cherry-picked contributor
commit: the original keyed the cache on the max mtime of only the TOP-LEVEL
scan dirs, so adding/removing a skill inside a category subdir (which bumps
the category dir's mtime, not the root's) served a stale list indefinitely.
The signature now covers roots + immediate children (mirroring
hermes_cli/profiles.py::_count_skills) plus the disabled-set, with a short
TTL bounding in-place SKILL.md edit staleness.
"""

import json
import time

import pytest

import tools.skills_tool as st


@pytest.fixture(autouse=True)
def _fresh_cache(monkeypatch, tmp_path):
    """Isolate every test: clear the module cache and point the scan at
    an empty external-dirs list + a tmp skills root."""
    st._SKILLS_CACHE.clear()
    monkeypatch.setattr(st, "_skills_dir", lambda: tmp_path / "skills")
    monkeypatch.setattr(
        "agent.skill_utils.get_external_skills_dirs", lambda: []
    )
    monkeypatch.setattr(st, "_get_disabled_skill_names", lambda: set())
    yield
    st._SKILLS_CACHE.clear()


def _write_skill(root, category, name, description="a skill"):
    d = root / "skills" / category / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: {description}\n---\n# {name}\n",
        encoding="utf-8",
    )
    return d


def test_cache_hit_serves_copies_not_cache_objects(tmp_path):
    """Callers mutate the returned dicts (web_server annotates
    s['enabled']/s['usage']) — the cache must hand out per-call copies."""
    _write_skill(tmp_path, "cat-a", "skill-one")
    first = st._find_all_skills()
    assert [s["name"] for s in first] == ["skill-one"]

    # Mutate what the first caller got; the next (cached) call must be clean.
    first[0]["enabled"] = False
    first.append({"name": "junk"})

    second = st._find_all_skills()
    assert [s["name"] for s in second] == ["skill-one"]
    assert "enabled" not in second[0], "cache poisoned by caller mutation"
    assert second is not first


def test_disabled_and_full_views_cached_separately(tmp_path, monkeypatch):
    _write_skill(tmp_path, "cat-a", "skill-one")
    _write_skill(tmp_path, "cat-a", "skill-two")
    monkeypatch.setattr(st, "_get_disabled_skill_names", lambda: {"skill-two"})

    filtered = sorted(s["name"] for s in st._find_all_skills())
    everything = sorted(s["name"] for s in st._find_all_skills(skip_disabled=True))
    assert filtered == ["skill-one"]
    assert everything == ["skill-one", "skill-two"]


def test_skill_view_reuses_cached_recursive_path_index(tmp_path, monkeypatch):
    skill_dir = _write_skill(tmp_path, "cat-a", "disk-name")
    skill_md = skill_dir / "SKILL.md"
    skill_md.write_text(
        "---\nname: frontmatter-name\ndescription: cached\n---\n# Cached\n",
        encoding="utf-8",
    )
    from agent import skill_utils

    original_iter = skill_utils.iter_skill_index_files
    scan_calls = 0

    def counted_iter(*args, **kwargs):
        nonlocal scan_calls
        scan_calls += 1
        yield from original_iter(*args, **kwargs)

    monkeypatch.setattr(skill_utils, "iter_skill_index_files", counted_iter)

    listed = json.loads(st.skills_list())
    assert listed["success"] is True
    assert [skill["name"] for skill in listed["skills"]] == ["frontmatter-name"]
    assert scan_calls == 1

    viewed = json.loads(st.skill_view("frontmatter-name"))
    assert viewed["success"] is True
    assert viewed["name"] == "frontmatter-name"
    assert scan_calls == 1


def test_skill_view_ignores_deleted_cached_skill_path(tmp_path):
    skill_dir = _write_skill(tmp_path, "cat-a", "disk-name")
    skill_md = skill_dir / "SKILL.md"
    skill_md.write_text(
        "---\nname: stale-name\ndescription: stale content\n---\n# Stale\n",
        encoding="utf-8",
    )

    listed = json.loads(st.skills_list())
    assert listed["success"] is True
    assert [skill["name"] for skill in listed["skills"]] == ["stale-name"]

    skill_md.unlink()
    viewed = json.loads(st.skill_view("stale-name"))
    assert viewed["success"] is False
    assert "not found" in viewed["error"]
    assert "content" not in viewed


def test_interrupted_scan_does_not_publish_partial_cache(tmp_path, monkeypatch):
    skill_md = _write_skill(tmp_path, "cat-a", "first-skill") / "SKILL.md"
    yielded = []

    def interrupted_iter(*args, **kwargs):
        yielded.append(skill_md)
        yield skill_md
        raise InterruptedError("User sent a new message")

    monkeypatch.setattr(
        "agent.skill_utils.iter_skill_index_files", interrupted_iter
    )

    with pytest.raises(InterruptedError, match="User sent a new message"):
        st._find_all_skills()

    assert yielded == [skill_md]
    assert "filtered" not in st._SKILLS_CACHE


def test_quarantined_project_alias_precedes_lower_tier_alias(tmp_path, monkeypatch):
    project_root = tmp_path / "project-skills"
    project_skill = project_root / "cat-project" / "project-disk-name"
    project_skill.mkdir(parents=True)
    project_skill_md = project_skill / "SKILL.md"
    project_skill_md.write_text(
        "---\nname: shared-alias\ndescription: project\n---\n# Project\n",
        encoding="utf-8",
    )
    local_skill = _write_skill(tmp_path, "cat-local", "local-disk-name")
    (local_skill / "SKILL.md").write_text(
        "---\nname: shared-alias\ndescription: lower tier\n---\n# Lower tier content\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "agent.skill_utils.get_project_skills_dirs", lambda: [project_root]
    )
    monkeypatch.setattr(
        "agent.skill_utils.is_quarantined_project_skill",
        lambda skill_md: skill_md == project_skill_md,
    )

    viewed = json.loads(st.skill_view("shared-alias"))
    assert viewed["success"] is False
    assert "quarantined" in viewed["error"]
    assert "Lower tier content" not in json.dumps(viewed)
