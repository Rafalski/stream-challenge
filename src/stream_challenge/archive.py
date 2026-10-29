"""Store Kafka values without parsing or rewriting their contents."""

import logging
import os
import re
import shutil
import tempfile
import time
from pathlib import Path


logger = logging.getLogger("raw_ingester")


class ArchiveGuard:
    """Track one local writer's archive growth and periodically check free space."""

    def __init__(
        self, root: Path, max_files: int = 10000, max_bytes: int = 100 * 1024**2,
        min_free_bytes: int = 1024**3,
    ):
        """Include existing files in the budget before ingestion starts."""
        if max_files < 1 or max_bytes < 1 or min_free_bytes < 0:
            raise ValueError("archive budgets must be positive; the free-space reserve cannot be negative")
        self.root = root
        self.max_files = max_files
        self.max_bytes = max_bytes
        self.min_free_bytes = min_free_bytes
        self.files = 0
        self.bytes = 0
        self.free_bytes = 0
        self._next_space_check = 0.0
        self._warned: set[str] = set()
        root.mkdir(parents=True, exist_ok=True)

        def scan_failed(error):
            """Do not trust a budget if part of the archive cannot be read."""
            raise error

        # Scan once, then count our successful writes rather than rescanning each record.
        for directory, _, names in os.walk(root, onerror=scan_failed):
            for name in names:
                self.files += 1
                self.bytes += (Path(directory) / name).stat().st_size
                self.check()
        self.check()

    def check(self, new_bytes: int | None = None) -> None:
        """Reject new growth beyond the budget and warn once per pressure signal."""
        new_file = 1 if new_bytes is not None else 0
        files = self.files + new_file
        size = self.bytes + (new_bytes or 0)
        if files > self.max_files:
            raise RuntimeError(f"archive_file_limit: {files} would exceed {self.max_files}; review the archive budget")
        if size > self.max_bytes:
            raise RuntimeError(f"archive_byte_limit: {size} would exceed {self.max_bytes}; review the archive budget")

        # Disk space can change even while no messages arrive.
        now = time.monotonic()
        if now >= self._next_space_check:
            self.free_bytes = shutil.disk_usage(self.root).free
            self._next_space_check = now + 5
        if self.free_bytes < self.min_free_bytes:
            raise RuntimeError("archive_free_space_limit: free space is below the configured reserve")

        warnings = {
            "file_count": files >= self.max_files * 0.8,
            "archive_bytes": size >= self.max_bytes * 0.8,
            "free_space": self.free_bytes < self.min_free_bytes * 2,
        }
        for reason, reached in warnings.items():
            if reached and reason not in self._warned:
                self._warned.add(reason)
                logger.warning("archive_budget_warning", extra={"details": {
                    "reason": reason, "files": files, "bytes": size, "free_bytes": self.free_bytes,
                }})

    def record_written(self, size: int) -> None:
        """Update the counters only after a new file has been published."""
        self.files += 1
        self.bytes += size


def archive_record(root: Path, topic: str, partition: int, offset: int, payload: bytes | None,
                   guard: ArchiveGuard | None = None) -> Path:
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

    if guard is not None:
        guard.check(new_bytes=len(content))

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
        if guard is not None:
            guard.record_written(len(content))
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination
