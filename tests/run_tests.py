#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _print_err(message: str) -> None:
    sys.stderr.write(message.rstrip() + "\n")


def _run(cmd: list[str], *, cwd: Path) -> int:
    proc = subprocess.run(cmd, cwd=cwd, text=True)
    return proc.returncode


def _mode_from_env_or_arg(mode: str | None) -> str:
    if mode:
        return mode
    return os.environ.get("TEST_MODE", "demo")


def _validate_mode(mode: str) -> None:
    if mode not in {"demo", "production"}:
        raise SystemExit("Invalid mode. Use --mode demo|production or set TEST_MODE=demo|production.")


def run_demo() -> int:
    code = 0
    code |= _run([sys.executable, "-m", "compileall", "-q", "."], cwd=REPO_ROOT)
    code |= _run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py", "-q"],
        cwd=REPO_ROOT,
    )
    return code


def run_production() -> int:
    if os.environ.get("PRODUCTION_TESTS_CONFIRM") != "1":
        _print_err(
            "Production-mode tests are guarded.\n"
            "Set PRODUCTION_TESTS_CONFIRM=1 to confirm you intend to run real integration checks.\n"
            "Example:\n"
            "  TEST_MODE=production PRODUCTION_TESTS_CONFIRM=1 python3 tests/run_tests.py\n"
        )
        return 2

    missing_tools = []
    try:
        import pandas  # noqa: F401
    except ModuleNotFoundError:
        missing_tools.append("pandas (pip)")
    try:
        import pyarrow  # noqa: F401
    except ModuleNotFoundError:
        missing_tools.append("pyarrow (pip)")

    if missing_tools:
        _print_err(
            "Missing optional dependencies required for production-mode checks:\n"
            f"  - {', '.join(missing_tools)}\n\n"
            "Install them with:\n"
            "  python -m venv .venv\n"
            "  . .venv/bin/activate\n"
            "  pip install -r requirements.txt\n\n"
            "Then rerun:\n"
            "  TEST_MODE=production PRODUCTION_TESTS_CONFIRM=1 python3 tests/run_tests.py\n"
        )
        return 2

    tmp_out = REPO_ROOT / "data" / "processed" / "_prod_check"
    tmp_out.mkdir(parents=True, exist_ok=True)
    code = _run(
        [sys.executable, str(REPO_ROOT / "pipelines" / "pipeline.py"), "--format", "parquet", "--output-dir", str(tmp_out)],
        cwd=REPO_ROOT,
    )
    if code != 0:
        return code

    parquet_path = tmp_out / "events" / "events.parquet"
    if not parquet_path.exists():
        _print_err(f"Expected Parquet output not found: {parquet_path}")
        return 1

    _print_err(f"Production check OK: wrote {parquet_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run repository tests in demo or production mode.")
    parser.add_argument("--mode", choices=["demo", "production"], help="Execution mode.")
    args = parser.parse_args(argv)

    mode = _mode_from_env_or_arg(args.mode)
    _validate_mode(mode)

    if mode == "demo":
        return run_demo()
    return run_production()


if __name__ == "__main__":
    raise SystemExit(main())
