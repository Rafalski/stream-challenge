import pytest

from stream_challenge.archive import archive_record


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
