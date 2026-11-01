"""Read the real sink's file layout without starting Docker."""

import json

import pytest

from stream_challenge.results import load_rows, minute_key, select_rows


def test_finished_files_only_and_duplicate_rows_are_preserved(tmp_path):
    """Pending output stays hidden; independent rows are never merged."""
    row = {"postcode": "SW19", "window_start": "2026-11-01T19:46:00Z", "pageview_count": 3}
    (tmp_path / "part-a.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    nested = tmp_path / "2026-11-01"
    nested.mkdir()
    (nested / "part-b.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    (tmp_path / ".part-c.jsonl.inprogress.id").write_text("not finished", encoding="utf-8")

    rows = load_rows(tmp_path)

    assert len(rows) == 2
    assert [item.count for item in rows] == [3, 3]
    assert {item.path.name for item in rows} == {"part-a.jsonl", "part-b.jsonl"}


def test_postcode_and_utc_minute_filters(tmp_path):
    """Filter literal output rows by postcode and the complete UTC minute."""
    rows = [
        {"postcode": "SW19", "window_start": "2026-11-01T19:46:00Z", "pageview_count": 3},
        {"postcode": "M1", "window_start": "2026-11-01T19:46:00Z", "pageview_count": 1},
        {"postcode": "SW19", "window_start": "2026-11-01T19:47:00Z", "pageview_count": 2},
    ]
    (tmp_path / "part-a.jsonl").write_text("\n".join(map(json.dumps, rows)), encoding="utf-8")

    selected = select_rows(load_rows(tmp_path), "SW19", "2026-11-01T19:46Z")

    assert [(row.postcode, row.minute, row.count) for row in selected] == [
        ("SW19", "2026-11-01T19:46:00Z", 3),
    ]


@pytest.mark.parametrize("minute", ["19:46", "2026-11-01T19:46:15Z", "2026-02-30T19:46"])
def test_ambiguous_or_invalid_minutes_are_rejected(minute):
    """A time of day alone must not mix different dates."""
    with pytest.raises(ValueError, match="YYYY-MM-DDTHH:MM"):
        minute_key(minute)


def test_invalid_output_reports_file_and_line(tmp_path):
    """A broken finished file is visible rather than silently omitted."""
    (tmp_path / "part-broken.jsonl").write_text("{broken}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="part-broken.jsonl:1"):
        load_rows(tmp_path)
