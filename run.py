#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import argparse
import json
import os
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parent
DBT_PROJECT_DIR = REPO_ROOT / "dbt" / "local_lakehouse"
DBT_PROFILES_DIR = REPO_ROOT / "dbt"
DUCKDB_PATH = REPO_ROOT / "duckdb" / "warehouse.duckdb"
RAW_DIR = REPO_ROOT / "data" / "raw"
REPORT_SCRIPT = REPO_ROOT / "reports" / "build_report.py"


def _print_header(title: str) -> None:
    line = "=" * len(title)
    print(f"\n{title}\n{line}")


def _run_cmd(cmd: list[str], env: dict[str, str] | None = None) -> None:
    print(" ".join(cmd))
    subprocess.run(cmd, check=True, env=env)


def _ensure_dirs() -> None:
    (REPO_ROOT / "duckdb").mkdir(exist_ok=True)
    (REPO_ROOT / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (REPO_ROOT / "reports" / "outputs").mkdir(parents=True, exist_ok=True)


def _dbt_env() -> dict[str, str]:
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_PROFILES_DIR)
    env["DUCKDB_PATH"] = str(DUCKDB_PATH)
    return env


def _dbt_vars() -> str:
    # Absolute forward-slash path helps on Windows
    raw_dir = str(RAW_DIR.resolve()).replace("\\", "/")
    return json.dumps({"raw_dir": raw_dir})


def cmd_ingest(args: argparse.Namespace) -> None:
    _print_header("Ingest (generate synthetic data)")
    _ensure_dirs()
    script = REPO_ROOT / "ingestion" / "generate_data.py"
    cmd = [
        sys.executable,
        str(script),
        "--out",
        str(RAW_DIR),
        "--days",
        str(args.days),
        "--customers",
        str(args.customers),
        "--products",
        str(args.products),
        "--orders",
        str(args.orders),
        "--seed",
        str(args.seed),
    ]
    _run_cmd(cmd)


def cmd_dbt_build(_: argparse.Namespace) -> None:
    _print_header("dbt build")
    _ensure_dirs()
    env = _dbt_env()
    _run_cmd(
        [
            "dbt",
            "build",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROFILES_DIR),
            "--vars",
            _dbt_vars(),
        ],
        env=env,
    )


def cmd_dbt_docs(_: argparse.Namespace) -> None:
    _print_header("dbt docs generate")
    _ensure_dirs()
    env = _dbt_env()
    _run_cmd(
        [
            "dbt",
            "docs",
            "generate",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROFILES_DIR),
            "--vars",
            _dbt_vars(),
        ],
        env=env,
    )


def cmd_report(_: argparse.Namespace) -> None:
    _print_header("Build report artifact")
    _ensure_dirs()
    _run_cmd([sys.executable, str(REPORT_SCRIPT), "--db", str(DUCKDB_PATH)])


def cmd_clean(_: argparse.Namespace) -> None:
    _print_header("Clean generated artifacts")
    import shutil

    if DUCKDB_PATH.exists():
        DUCKDB_PATH.unlink()
        print(f"Removed {DUCKDB_PATH}")

    targets = [
        RAW_DIR,
        REPO_ROOT / "reports" / "outputs",
        DBT_PROJECT_DIR / "target",
        DBT_PROJECT_DIR / "dbt_packages",
        DBT_PROJECT_DIR / "logs",
    ]
    for p in targets:
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
            print(f"Removed {p}")


def cmd_demo(args: argparse.Namespace) -> None:
    cmd_ingest(args)
    cmd_dbt_build(args)
    cmd_report(args)
    cmd_dbt_docs(args)

    _print_header("Done")
    print("Next steps:")
    print("  - Open reports/outputs/report.md")
    print("  - Explore DuckDB file: duckdb/warehouse.duckdb")
    print("  - Serve docs: cd dbt/local_lakehouse && dbt docs serve --profiles-dir ..")


def main() -> None:
    parser = argparse.ArgumentParser(description="Local lakehouse demo runner (DuckDB + dbt + Python).")
    sub = parser.add_subparsers(dest="command", required=True)

    p_demo = sub.add_parser("demo", help="Run ingest -> dbt build -> report -> dbt docs generate")
    p_demo.add_argument("--days", type=int, default=30)
    p_demo.add_argument("--customers", type=int, default=500)
    p_demo.add_argument("--products", type=int, default=100)
    p_demo.add_argument("--orders", type=int, default=3000)
    p_demo.add_argument("--seed", type=int, default=7)
    p_demo.set_defaults(func=cmd_demo)

    p_ing = sub.add_parser("ingest", help="Generate raw data files")
    p_ing.add_argument("--days", type=int, default=30)
    p_ing.add_argument("--customers", type=int, default=500)
    p_ing.add_argument("--products", type=int, default=100)
    p_ing.add_argument("--orders", type=int, default=3000)
    p_ing.add_argument("--seed", type=int, default=7)
    p_ing.set_defaults(func=cmd_ingest)

    p_build = sub.add_parser("build", help="Run dbt build")
    p_build.set_defaults(func=cmd_dbt_build)

    p_docs = sub.add_parser("docs", help="Generate dbt docs")
    p_docs.set_defaults(func=cmd_dbt_docs)

    p_rep = sub.add_parser("report", help="Generate a small markdown report from marts")
    p_rep.set_defaults(func=cmd_report)

    p_clean = sub.add_parser("clean", help="Remove generated data and build outputs")
    p_clean.set_defaults(func=cmd_clean)

    args = parser.parse_args()
    try:
        args.func(args)
    except FileNotFoundError as e:
        print(f"\nERROR: {e}\n", file=sys.stderr)
        print("It looks like a required command isn't installed (e.g., 'dbt').", file=sys.stderr)
        print("Try: pip install -r requirements.txt", file=sys.stderr)
        sys.exit(2)
    except subprocess.CalledProcessError as e:
        print(f"\nERROR: command failed with exit code {e.returncode}\n", file=sys.stderr)
        sys.exit(e.returncode)


if __name__ == "__main__":
    main()

