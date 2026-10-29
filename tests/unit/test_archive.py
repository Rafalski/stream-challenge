import pytest

from stream_challenge.archive import ArchiveGuard, archive_record


@pytest.mark.parametrize("payload", [b' {"postcode": "SW19"}\n', b"not JSON\n\n", b"\xff\x00", b""])
def test_payload_is_preserved_exactly(tmp_path, payload):
    path = archive_record(tmp_path, "pageviews", 0, 12, payload)
    assert path == tmp_path / "pageviews" / "0" / "12.bin"
    assert path.read_bytes() == payload


def test_null_and_empty_values_are_distinguishable(tmp_path):
    null_path = archive_record(tmp_path, "pageviews", 0, 0, None)
    empty_path = archive_record(tmp_path, "pageviews", 0, 1, b"")
    assert null_path.suffix == ".null"
    assert empty_path.suffix == ".bin"
    assert null_path.read_bytes() == empty_path.read_bytes() == b""


def test_replay_reuses_the_same_file(tmp_path):
    first = archive_record(tmp_path, "pageviews", 0, 0, b"original")
    replay = archive_record(tmp_path, "pageviews", 0, 0, b"original")
    assert replay == first
    assert list(tmp_path.rglob("*.bin")) == [first]


@pytest.mark.parametrize("replacement", [b"different", None])
def test_conflicting_replay_does_not_overwrite_evidence(tmp_path, replacement):
    path = archive_record(tmp_path, "pageviews", 0, 0, b"original")
    with pytest.raises(FileExistsError):
        archive_record(tmp_path, "pageviews", 0, 0, replacement)
    assert path.read_bytes() == b"original"


def test_failed_publish_cleans_up_the_temporary_file(tmp_path, monkeypatch):
    def fail_replace(source, destination):
        raise OSError("write failed")

    monkeypatch.setattr("stream_challenge.archive.os.replace", fail_replace)
    with pytest.raises(OSError, match="write failed"):
        archive_record(tmp_path, "pageviews", 0, 0, b"original")
    assert not any(path.is_file() for path in tmp_path.rglob("*"))


def test_topic_cannot_escape_the_archive_directory(tmp_path):
    with pytest.raises(ValueError):
        archive_record(tmp_path, "../outside", 0, 0, b"original")


@pytest.mark.parametrize(("max_files", "max_bytes"), [(1, 100), (100, 8)])
def test_archive_budgets_allow_replay_but_block_new_growth(tmp_path, max_files, max_bytes):
    guard = ArchiveGuard(tmp_path, max_files=max_files, max_bytes=max_bytes, min_free_bytes=0)
    first = archive_record(tmp_path, "pageviews", 0, 0, b"original", guard=guard)
    assert archive_record(tmp_path, "pageviews", 0, 0, b"original", guard=guard) == first
    with pytest.raises(RuntimeError, match="archive_.*_limit"):
        archive_record(tmp_path, "pageviews", 0, 1, b"next", guard=guard)
    assert first.read_bytes() == b"original"
    assert not (first.parent / "1.bin").exists()


def test_guard_counts_existing_files_and_warns_before_the_limit(tmp_path, caplog):
    archive_record(tmp_path, "pageviews", 0, 0, b"original")
    guard = ArchiveGuard(tmp_path, max_files=2, min_free_bytes=0)
    assert guard.files == 1
    archive_record(tmp_path, "pageviews", 0, 1, b"next", guard=guard)
    assert "archive_budget_warning" in caplog.text


def test_low_free_space_stops_before_archiving(tmp_path, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr("stream_challenge.archive.shutil.disk_usage", lambda path: SimpleNamespace(free=99))
    with pytest.raises(RuntimeError, match="archive_free_space_limit"):
        ArchiveGuard(tmp_path, min_free_bytes=100)
    assert not any(tmp_path.iterdir())
