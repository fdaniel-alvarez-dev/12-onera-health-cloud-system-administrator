from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

ALLOWED_EVENT_TYPES = {"signup", "login", "purchase"}


@dataclass(frozen=True)
class Event:
    event_id: int
    user_id: int
    event_type: str
    event_ts: str


class ValidationError(ValueError):
    pass


def _parse_iso8601(value: str) -> None:
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValidationError(
            f"Invalid ISO-8601 timestamp for event_ts: {value!r} "
            "(expected e.g. 2026-01-31T12:34:56+00:00)"
        ) from exc


def _parse_int(field_name: str, raw: str) -> int:
    try:
        value = int(str(raw).strip())
    except Exception as exc:
        raise ValidationError(f"Invalid integer for {field_name}: {raw!r}") from exc
    if value < 1:
        raise ValidationError(f"{field_name} must be >= 1 (got {value})")
    return value


def read_events_csv(path: Path) -> list[Event]:
    if not path.exists():
        raise FileNotFoundError(
            f"Input not found: {path}. Expected a CSV with columns: "
            "event_id,user_id,event_type,event_ts"
        )

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValidationError("CSV has no header row")

        required = ["event_id", "user_id", "event_type", "event_ts"]
        missing = [name for name in required if name not in reader.fieldnames]
        if missing:
            raise ValidationError(f"CSV missing required columns: {', '.join(missing)}")

        events: list[Event] = []
        for row_idx, row in enumerate(reader, start=2):
            event_id = _parse_int("event_id", row["event_id"])
            user_id = _parse_int("user_id", row["user_id"])
            event_type = str(row["event_type"]).strip()
            if event_type not in ALLOWED_EVENT_TYPES:
                raise ValidationError(
                    f"Invalid event_type on line {row_idx}: {event_type!r} "
                    f"(allowed: {', '.join(sorted(ALLOWED_EVENT_TYPES))})"
                )

            event_ts = str(row["event_ts"]).strip()
            _parse_iso8601(event_ts)
            events.append(
                Event(event_id=event_id, user_id=user_id, event_type=event_type, event_ts=event_ts)
            )

    return events


def write_events_csv(events: list[Event], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["event_id", "user_id", "event_type", "event_ts"])
        for ev in events:
            writer.writerow([ev.event_id, ev.user_id, ev.event_type, ev.event_ts])


def write_events_parquet(events: list[Event], out_path: Path) -> None:
    try:
        import pandas as pd  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Parquet output requires optional dependencies. Install them first:\n"
            "  python -m venv .venv\n"
            "  . .venv/bin/activate\n"
            "  pip install -r requirements.txt\n"
            "Then rerun with: python pipelines/pipeline.py --format parquet"
        ) from exc

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([ev.__dict__ for ev in events])
    df.to_parquet(out_path, index=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a tiny, deterministic events pipeline.")
    parser.add_argument("--input", default="data/raw/events.csv", help="Path to input CSV.")
    parser.add_argument(
        "--output-dir", default="data/processed", help="Output directory for pipeline artifacts."
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output format for processed data.",
    )
    args = parser.parse_args(argv)

    events = read_events_csv(Path(args.input))
    events = sorted(events, key=lambda e: (e.event_id, e.user_id, e.event_type, e.event_ts))

    out_dir = Path(args.output_dir)
    if args.format == "csv":
        out_path = out_dir / "events" / "events.csv"
        write_events_csv(events, out_path)
    else:
        out_path = out_dir / "events" / "events.parquet"
        write_events_parquet(events, out_path)

    print(f"Wrote {out_path} ({len(events)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
