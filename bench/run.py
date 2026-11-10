"""Benchmark matchstream on synthetic data. Writes bench/results.md.

    python bench/run.py [--quick]
"""
import random
import sys
import time
from pathlib import Path

import pandas as pd

import matchstream as ms

QUICK = "--quick" in sys.argv
OUT = Path(__file__).parent / "results.md"


def keyed_data(n: int, seed: int = 1):
    rng = random.Random(seed)
    a = pd.DataFrame({"ref": [f"INV-{i:08d}" for i in range(n)],
                      "amount": [round(rng.uniform(1, 10_000), 2) for _ in range(n)]})
    keep, add = int(n * 0.95), int(n * 0.05)
    b = pd.concat([a.iloc[:keep], pd.DataFrame({"ref": [f"EXT-{i:08d}" for i in range(add)],
                                                "amount": [9999.0] * add})], ignore_index=True)
    b.loc[: n // 1000, "amount"] += 5  # 0.1% amount mismatches
    return a, b


def pandas_reconcile(a, b):
    m = a.merge(b, on="ref", how="outer", suffixes=("_a", "_b"), indicator=True)
    diff = (m.amount_b - m.amount_a).abs()
    m["status"] = m["_merge"].astype(str).map({"left_only": "only_in_a", "right_only": "only_in_b", "both": "matched"})
    m.loc[(m.status == "matched") & (diff > 0.01), "status"] = "amount_mismatch"
    return m.status.value_counts().to_dict()


def window_data(n: int, seed: int = 2):
    """Bank vs ledger with no shared key: same amounts, ledger dates 0-2 days later,
    and 20% recurring amounts (same value, different days) to make it ambiguous."""
    rng = random.Random(seed)
    start = pd.Timestamp("2025-01-01")
    amounts = [99.0 if rng.random() < 0.2 else round(rng.uniform(1, 5_000), 2) for _ in range(n)]
    days = [rng.randrange(365) for _ in range(n)]
    bank = pd.DataFrame({"id": range(n), "date": [start + pd.Timedelta(d, "D") for d in days], "amount": amounts})
    ledger = pd.DataFrame({"id": range(n), "posted": [start + pd.Timedelta(d + rng.randrange(3), "D") for d in days],
                           "debit": amounts}).sample(frac=1, random_state=seed)
    return bank, ledger


lines = ["| scenario | rows (a × b) | matchstream | pandas merge | notes |", "|---|---:|---:|---:|---|"]
for n in ([100_000] if QUICK else [100_000, 1_000_000]):
    a, b = keyed_data(n)
    t0 = time.perf_counter()
    expected = pandas_reconcile(a, b)
    t_pd = time.perf_counter() - t0
    for normalize in (False, True):
        with ms.reconcile(a, b, on="ref", amount="amount", tolerance=0.01, normalize=normalize) as r:
            secs, counts = r.summary["seconds"], {k: v for k, v in r.summary["counts"].items() if v}
        assert counts == expected, (counts, expected)
        lines.append(f"| key match, normalize={normalize} | {n:,} × {len(b):,} | {secs:.2f}s | {t_pd:.2f}s | "
                     f"same counts as pandas |")
    print(lines[-1])

for n in ([20_000] if QUICK else [100_000]):
    bank, ledger = window_data(n)
    with ms.reconcile(bank, ledger, amount=("amount", "debit"), date=("date", "posted"), days=3) as r:
        df = r.df("matched")
        secs, matched = r.summary["seconds"], len(df)
    correct = int((df.id_a == df.id_b).sum())
    unique = df[df.amount_a != 99.0]
    lines.append(f"| amount + date, no key | {n:,} × {n:,} | {secs:.2f}s | n/a | {matched / n:.1%} paired; "
                 f"true partner for {(unique.id_a == unique.id_b).mean():.1%} of unique amounts, "
                 f"{correct / n:.1%} overall (20% are identical recurring amounts) |")
    print(lines[-1])

OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
