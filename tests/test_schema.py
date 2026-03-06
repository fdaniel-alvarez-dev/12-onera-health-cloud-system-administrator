import tempfile
import unittest
from pathlib import Path

from pipelines.pipeline import ValidationError, read_events_csv, write_events_csv


class TestSchema(unittest.TestCase):
    def test_schema_validation_passes(self) -> None:
        events = read_events_csv(Path("data/raw/events.csv"))
        self.assertGreater(len(events), 0)

    def test_schema_rejects_invalid_event_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "events.csv"
            csv_path.write_text(
                "event_id,user_id,event_type,event_ts\n"
                "1,1,not-valid,2026-01-31T12:34:56+00:00\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValidationError):
                read_events_csv(csv_path)

    def test_schema_rejects_missing_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "events.csv"
            csv_path.write_text("event_id,user_id,event_ts\n1,1,2026-01-31T12:34:56+00:00\n", encoding="utf-8")
            with self.assertRaises(ValidationError):
                read_events_csv(csv_path)

    def test_output_is_deterministic(self) -> None:
        events = read_events_csv(Path("data/raw/events.csv"))
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path1 = Path(tmpdir) / "out1.csv"
            out_path2 = Path(tmpdir) / "out2.csv"
            write_events_csv(sorted(events, key=lambda e: e.event_id), out_path1)
            write_events_csv(sorted(events, key=lambda e: e.event_id), out_path2)
            self.assertEqual(out_path1.read_text(encoding="utf-8"), out_path2.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
