"""Store Kafka values without parsing or rewriting their contents."""

import os
import re
import tempfile
from pathlib import Path


def archive_record(root: Path, topic: str, partition: int, offset: int, payload: bytes | None) -> Path:
    """Save one value by Kafka coordinates, reusing matching replays."""
    # Keep the Kafka coordinates inside the archive directory.
    if not re.fullmatch(r"[A-Za-z0-9._-]+", topic) or topic in (".", ".."):
        raise ValueError("invalid topic name")
    if partition < 0 or offset < 0:
        raise ValueError("partition and offset must be non-negative")

    directory = root / topic / str(partition)
    directory.mkdir(parents=True, exist_ok=True)
    suffix = ".null" if payload is None else ".bin"
    destination = directory / f"{offset}{suffix}"
    other_kind = directory / f"{offset}{'.bin' if payload is None else '.null'}"
    content = b"" if payload is None else payload

    # A replay should find the same bytes, not quietly replace old evidence.
    if other_kind.exists():
        raise FileExistsError(f"archive conflict at {topic}/{partition}/{offset}")
    if destination.exists():
        if destination.read_bytes() != content:
            raise FileExistsError(f"archive conflict at {topic}/{partition}/{offset}")
        return destination

    # Publish the file only when the whole payload has been written.
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=directory, prefix=f".{offset}-", suffix=".tmp", delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination
