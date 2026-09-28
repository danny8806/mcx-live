"""Canonical database health command."""
from __future__ import annotations

import argparse
from .validate_trade_integrity import validate


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="trading.db")
    args = parser.parse_args()
    passed, report = validate(args.db)
    print(f"SQLite integrity: {'PASS' if report['DATABASE'] == ['ok'] else 'FAIL'}")
    print(f"Foreign keys: {'PASS' if not report['INVALID FK'] else 'FAIL'}")
    print(f"Lifecycle checks: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
