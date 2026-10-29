"""Readable local console logs, with JSON available for automation."""

import json
import logging
import os
from datetime import datetime, timezone


RESET = "\033[0m"
CYAN = "\033[96m"
COLOURS = {
    # Each entry gives the message colour and a contrasting level badge.
    "INFO": ("\033[94m", "\033[44;97m"),
    "STEP": ("\033[93m", "\033[43;30m"),
    "OK": ("\033[92m", "\033[42;30m"),
    "WARN": ("\033[95m", "\033[45;97m"),
    "ERROR": ("\033[91m", "\033[41;97m"),
    "DEBUG": ("\033[90m", "\033[100;97m"),
}


class JsonFormatter(logging.Formatter):
    """Keep machine-readable logs on one line, without colour codes."""

    def format(self, record: logging.LogRecord) -> str:
        """Add the common fields without including event payloads."""
        return json.dumps({
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "component": record.name,
            "event": record.getMessage(),
            **getattr(record, "details", {}),
        })


class ConsoleFormatter(logging.Formatter):
    """Show a compact timestamp, level badge and useful record details."""

    def __init__(self, colour: bool = True):
        """Allow colour to be switched off when output is redirected."""
        super().__init__()
        self.colour = colour

    def format(self, record: logging.LogRecord) -> str:
        """Render one console entry without changing its logging severity."""
        event = record.getMessage()

        # STEP and OK are display labels; JSON keeps standard logging levels.
        if record.levelno >= logging.ERROR:
            label = "ERROR"
        elif record.levelno >= logging.WARNING:
            label = "WARN"
        elif event in ("record_published", "record_archived") or event.endswith("_finished"):
            label = "OK"
        elif event.endswith("_started"):
            label = "STEP"
        elif record.levelno <= logging.DEBUG:
            label = "DEBUG"
        else:
            label = "INFO"

        timestamp = datetime.fromtimestamp(record.created, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        details = " ".join(
            f"{key}={json.dumps(value, ensure_ascii=False)}"
            for key, value in getattr(record, "details", {}).items()
        )
        message = f"[{record.name}] {event}" + (f" {details}" if details else "")
        badge = f"[{label:^5}]"

        # Colour only the badge background, so long lines stay easy to read.
        if self.colour:
            foreground, background = COLOURS[label]
            if event == "record_archived" and record.levelno < logging.WARNING:
                foreground = CYAN
            badge = f"{background}{badge}{RESET}"
            message = f"{foreground}{message}{RESET}"

        return f"[{timestamp}] {badge} {message}"


def configure_logging() -> None:
    """Select the local display or JSON using environment settings."""
    log_format = os.environ.get("LOG_FORMAT", "console").lower()
    colour_mode = os.environ.get("LOG_COLOUR", "auto").lower()
    if log_format not in ("console", "json"):
        raise ValueError("LOG_FORMAT must be console or json")
    if colour_mode not in ("auto", "always", "never"):
        raise ValueError("LOG_COLOUR must be auto, always or never")

    handler = logging.StreamHandler()
    if log_format == "json":
        formatter = JsonFormatter()
    else:
        colour = colour_mode == "always" or (colour_mode == "auto" and handler.stream.isatty())
        formatter = ConsoleFormatter(colour)

    handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[handler])
