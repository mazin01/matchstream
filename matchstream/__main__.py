"""matchstream A B [--on ref | --on ref:reference] [--amount amount[:debit]] [--tolerance 0.01|1%]
                  [--date date[:posted] --days 3] [--fuzzy 0.9] [--out result.csv|.parquet]"""
import argparse
import json
import sys

from . import MatchError, reconcile


def _pair(s):
    if s is None:
        return None
    left, _, right = s.partition(":")
    return (left, right) if right else left


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="matchstream", description="Reconcile two CSV/Parquet tables")
    ap.add_argument("a")
    ap.add_argument("b")
    ap.add_argument("--on", action="append", help="key column, or left:right; repeat for a composite key")
    ap.add_argument("--amount", help="amount column, or left:right")
    ap.add_argument("--tolerance", default="0", help="absolute (0.01) or percent (1%%)")
    ap.add_argument("--date", help="date column, or left:right (matching without a key)")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--fuzzy", type=float, help="Jaro-Winkler threshold for leftover keys, e.g. 0.9")
    ap.add_argument("--raw-keys", action="store_true", help="compare keys exactly, without normalization")
    ap.add_argument("--out", help="write the full result to .csv or .parquet")
    x = ap.parse_args(argv)
    on = None if not x.on else [_pair(k) for k in x.on] if len(x.on) > 1 else _pair(x.on[0])
    tolerance = x.tolerance if x.tolerance.endswith("%") else float(x.tolerance)
    try:
        with reconcile(x.a, x.b, on=on, amount=_pair(x.amount), tolerance=tolerance, date=_pair(x.date),
                       days=x.days, fuzzy=x.fuzzy, normalize=not x.raw_keys) as r:
            if x.out:
                (r.to_parquet if x.out.lower().endswith(".parquet") else r.to_csv)(x.out)
            sys.stdout.reconfigure(encoding="utf-8")
            print(json.dumps(r.summary, ensure_ascii=False, indent=2))
    except MatchError as e:
        print(json.dumps({"error": e.code, "message": str(e), **e.details}, ensure_ascii=False), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
