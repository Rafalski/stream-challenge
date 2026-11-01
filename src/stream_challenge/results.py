"""Preview finished aggregate files without changing or re-aggregating them."""

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class ResultRow:
    """Keep each result's location available for inspection."""

    postcode: str
    minute: str
    count: int
    path: Path
    line: int


def minute_key(value: str) -> str:
    """Require a complete UTC minute, with an optional trailing Z."""
    value = value.removesuffix("Z")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M")
        if parsed.isoformat(timespec="minutes") != value:
            raise ValueError
    except ValueError:
        raise ValueError("minute must be YYYY-MM-DDTHH:MM in UTC, optionally ending in Z") from None
    return value


def load_rows(directory: Path) -> list[ResultRow]:
    """Read finished parts only, including any date subdirectories."""
    if not directory.is_dir():
        raise ValueError(f"result directory does not exist: {directory}")

    rows = []
    for path in sorted(directory.rglob("part-*.jsonl")):
        with path.open(encoding="utf-8") as source:
            for line, text in enumerate(source, 1):
                try:
                    record = json.loads(text)
                    if not isinstance(record, dict) or set(record) != {"postcode", "window_start", "pageview_count"}:
                        raise ValueError
                    postcode, minute, count = record["postcode"], record["window_start"], record["pageview_count"]
                    if not isinstance(postcode, str) or not postcode.strip():
                        raise ValueError
                    if not isinstance(minute, str) or not minute.endswith(":00Z"):
                        raise ValueError
                    minute_key(minute[:-4])
                    if type(count) is not int or count <= 0:
                        raise ValueError
                except (ValueError, TypeError):
                    raise ValueError(f"invalid aggregate at {path}:{line}") from None
                rows.append(ResultRow(postcode, minute, count, path, line))

    # Preserve duplicates: a viewer must not hide replayed or conflicting output.
    return sorted(rows, key=lambda row: (row.minute, row.postcode, str(row.path), row.line))


def select_rows(rows: list[ResultRow], postcode: str = "", minute: str = "") -> list[ResultRow]:
    """Match postcode exactly and minute in UTC; empty filters show all rows."""
    minute = minute_key(minute) if minute else ""
    return [row for row in rows if (not postcode or row.postcode == postcode)
            and (not minute or row.minute[:16] == minute)]


def table_line(row: ResultRow) -> str:
    """Keep terminal control characters out of the table display."""
    postcode = "".join(char if char.isprintable() else "?" for char in row.postcode)
    return f"{row.minute:<22} {postcode[:24]:<24} {row.count:>10}"


def print_table(rows: list[ResultRow], directory: Path, limit: int, colour: bool) -> None:
    """Print an ordinary copyable table, with no terminal navigation required."""
    title = "AGGREGATE RESULTS | UTC"
    print(f"\033[100;97m {title} \033[0m" if colour else title)
    print(f"Directory: {directory}")
    print(f"{len(rows)} matching rows | {sum(row.count for row in rows)} pageviews in these rows")
    print(f"{'WINDOW START':<22} {'POSTCODE':<24} {'PAGEVIEWS':>10}")
    print("-" * 58)
    for row in rows[:limit]:
        print(table_line(row))
    if not rows:
        print("No matching finished results. Check filters and checkpoint progress.")
    elif len(rows) > limit:
        print(f"Showing {limit} of {len(rows)} rows; narrow the filters or increase --limit.")


def viewer_app(directory: Path, postcode: str, minute: str, colour: bool):
    """Build a small three-section terminal screen with optional row details."""
    from prompt_toolkit import Application
    from prompt_toolkit.data_structures import Point
    from prompt_toolkit.filters import has_focus
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.layout import HSplit, Layout, VSplit, Window
    from prompt_toolkit.layout.controls import FormattedTextControl
    from prompt_toolkit.styles import Style
    from prompt_toolkit.widgets import Frame, Label, TextArea

    # Filters stay editable; applying them also picks up newly committed files.
    postcode_input = TextArea(text=postcode, height=1, multiline=False)
    minute_input = TextArea(text=minute, height=1, multiline=False)
    rows = []
    selected = 0
    status = ""
    detail = TextArea(text="Enter on a row shows its source file and JSON.", read_only=True, height=7)

    def refresh():
        nonlocal rows, selected, status
        try:
            updated = select_rows(load_rows(directory), postcode_input.text, minute_input.text)
        except (OSError, ValueError, UnicodeError) as error:
            # Clear stale rows so an unsuccessful refresh cannot look current.
            rows, selected, status = [], 0, str(error)
        else:
            rows, selected = updated, 0
            status = f"{len(rows)} rows | {sum(row.count for row in rows)} pageviews in displayed results"
        detail.text = "Enter on a row shows its source file and JSON."

    def table_text():
        if not rows:
            return [("", "No matching finished rows. Apply filters or refresh after a checkpoint.")]
        fragments = []
        for index, row in enumerate(rows):
            fragments.append(("class:selected" if index == selected else "", table_line(row) + "\n"))
        return fragments

    table = FormattedTextControl(table_text, focusable=True,
                                 get_cursor_position=lambda: Point(x=0, y=selected))
    bindings = KeyBindings()
    on_table = has_focus(table)

    @bindings.add("tab")
    def next_field(event):
        event.app.layout.focus_next()

    @bindings.add("s-tab")
    def previous_field(event):
        event.app.layout.focus_previous()

    @bindings.add("up", filter=on_table)
    @bindings.add("down", filter=on_table)
    def move(event):
        nonlocal selected
        step = -1 if event.key_sequence[-1].key == "up" else 1
        selected = max(0, min(len(rows) - 1, selected + step))

    @bindings.add("enter", eager=True)
    def enter(event):
        if not event.app.layout.has_focus(table):
            refresh()
            event.app.layout.focus(table)
        elif rows:
            row = rows[selected]
            payload = {"postcode": row.postcode, "window_start": row.minute, "pageview_count": row.count}
            source = row.path.relative_to(directory)
            detail.text = f"{source}:{row.line}\n" + json.dumps(payload, indent=2, ensure_ascii=False)

    @bindings.add("r", filter=on_table)
    @bindings.add("R", filter=on_table)
    def reload_rows(event):
        refresh()

    @bindings.add("1", filter=on_table)
    @bindings.add("2", filter=on_table)
    @bindings.add("3", filter=on_table)
    def focus_section(event):
        target = {"1": postcode_input, "2": minute_input, "3": table}[event.data]
        event.app.layout.focus(target)

    @bindings.add("escape")
    def return_to_table(event):
        event.app.layout.focus(table)

    @bindings.add("q", filter=on_table)
    @bindings.add("Q", filter=on_table)
    @bindings.add("c-c")
    def quit_viewer(event):
        event.app.exit()

    refresh()
    filters = VSplit([Frame(postcode_input, title="1 Postcode: blank = all"),
                      Frame(minute_input, title="2 UTC minute: YYYY-MM-DDTHH:MM")])
    body = HSplit([
        Label(" AGGREGATE RESULTS | Finished files only | UTC", style="class:heading"),
        Label(str(directory)), filters,
        Label(f"{'WINDOW START':<22} {'POSTCODE':<24} {'PAGEVIEWS':>10}", style="class:heading"),
        Window(table, wrap_lines=False), Frame(detail, title="Selected record"),
        Label(lambda: status),
        Label(" Tab focus | 1/2 filters | Up/Down row | Enter apply/view | R refresh | Q quit"),
    ])
    style = Style.from_dict({"": "bg:#18181b #e4e4e7", "heading": "bg:#27272a #fafafa bold",
                             "selected": "bg:#3f3f46 #fafafa bold", "frame.border": "#71717a"}) if colour else None
    return Application(layout=Layout(body, focused_element=table), key_bindings=bindings,
                       full_screen=True, style=style)


def main() -> int:
    """Select one run directory, then print a table or open the keyboard viewer."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="aggregate directory for one submission")
    parser.add_argument("--postcode", default="", help="exact postcode; omitted means all")
    parser.add_argument("--minute", default="", help="UTC YYYY-MM-DDTHH:MM, optionally ending in Z")
    parser.add_argument("--limit", type=int, default=50, help="maximum rows in a plain preview (default 50)")
    parser.add_argument("--interactive", action="store_true", help="open the optional keyboard viewer")
    parser.add_argument("--no-colour", action="store_true", help="disable colour styling")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")

    colour = not args.no_colour and "NO_COLOR" not in os.environ and sys.stdout.isatty()
    try:
        rows = select_rows(load_rows(args.directory), args.postcode, args.minute)
        if args.interactive:
            if not sys.stdin.isatty() or not sys.stdout.isatty():
                raise ValueError("interactive mode needs a terminal; omit --interactive for a plain table")
            try:
                import prompt_toolkit
            except ImportError:
                raise ValueError('interactive mode requires: python -m pip install -e ".[viewer]"') from None

            # Native Windows Python in mintty may need winpty's console bridge.
            terminal_errors = (OSError,)
            if os.name == "nt":
                from prompt_toolkit.output.win32 import NoConsoleScreenBufferError
                terminal_errors += (NoConsoleScreenBufferError,)
            try:
                viewer_app(args.directory, args.postcode, args.minute, colour).run()
            except terminal_errors:
                raise ValueError("cannot open the terminal screen; try Windows Terminal or winpty in Git Bash, "
                                 "or omit --interactive for a plain table") from None
        else:
            print_table(rows, args.directory, args.limit, colour)
    except (OSError, ValueError, UnicodeError) as error:
        print(f"results: {error}", file=sys.stderr)
        return 1
    except (EOFError, KeyboardInterrupt):
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
