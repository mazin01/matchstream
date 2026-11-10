# matchstream

**Compare two tables that should agree, in one call: match rows by key, or by value + date when there's no shared key. Built on DuckDB, with Arabic-safe keys and numbers.**

Two systems that describe the same things rarely agree exactly: orders and shipments, a stock count and the inventory system, a CRM export before and after a migration. Finding the differences looks like a join, until the details make a plain join wrong:

- **Keys that look equal but aren't:** `ORD-١٠١` vs `ORD-101`, or values wrapped in invisible right-to-left marks by a spreadsheet.
- **Numbers stored as text:** `"1,200"`, `"(5)"`, `"١٬٢٠٠"`.
- **Duplicate keys:** two shipments for the same order turn a plain join into a cross product.
- **No shared key at all:** dispatch records vs. delivery confirmations, where only the weight and roughly the date agree.

```python
import matchstream as ms

with ms.reconcile("orders.csv", "shipments.parquet", on=("order_id", "order"), amount=("qty", "units")) as r:
    print(r.summary)          # counts and totals per status
    r.df("amount_mismatch")   # pandas DataFrame
    r.to_csv("differences.csv")
```

```text
         status order_id_a qty_a order_b  units_b  amount_diff
        matched    ORD-١٠١ 1,200 ORD-101     1200          0.0
amount_mismatch    ORD-102    40 ORD-102       36         -4.0
        matched    ORD-103    15 ORD-103       15          0.0
      only_in_a    ORD-104     8     NaN     <NA>          NaN
      only_in_b        NaN   NaN ORD-109        5          NaN

{'mode': 'key', 'rows_a': 4, 'rows_b': 4,
 'counts': {'matched': 2, 'amount_mismatch': 1, 'only_in_a': 1, 'only_in_b': 1},
 'totals': {'matched': {'a': 1215.0, 'b': 1215.0}, 'amount_mismatch': {'a': 40.0, 'b': 36.0}, ...}}
```

Every row of both tables appears exactly once in the result, with a `status`, its original row numbers (`row_a`, `row_b`), the differences (`amount_diff`, `days_apart`, `similarity`), and all original columns suffixed `_a` / `_b`. `amount` is the name for whatever number should agree between the two sides: quantity, weight, count or price.

| status | meaning |
|---|---|
| `matched` | paired, and the values agree within tolerance (or no value was given) |
| `matched_fuzzy` | paired by a similar (not equal) key: `fuzzy=` |
| `amount_mismatch` | paired by key, but the values differ |
| `only_in_a` / `only_in_b` | no partner |

## Install

```bash
pip install "matchstream @ git+https://github.com/mazin01/matchstream"
```

Dependencies: `duckdb`, `pandas` and [`nuqta`](https://github.com/mazin01/nuqta) (Arabic text helpers). Add `pyarrow` to read or write Parquet from pandas.

## Matching modes

**By key.** `on="order_id"`, or `on=("order_id", "order")` when the names differ, or `on=["order_id", "warehouse"]` for a composite key (a list means several keys).
- Keys are compared after [`nuqta.normalize_key`](https://github.com/mazin01/nuqta): Arabic-Indic digits, invisible bidi marks, NBSPs, tatweel and spacing. Turn it off with `normalize=False`.
- Empty or null-like keys (`""`, `NaN`, `None`) never match each other.
- **Duplicate keys pair one-to-one**, the n-th smallest value with the n-th smallest. Three shipment lines against two order lines give two pairs and one `only_in_a`, never six rows.
- `amount=` with `tolerance=0.5` (absolute) or `tolerance="1%"` (of the left value) splits pairs into `matched` and `amount_mismatch`.
- `fuzzy=0.9` gives a second pass: leftover keys with Jaro-Winkler similarity ≥ 0.9 are paired one-to-one, best first (`ACME Trading` ↔ `ACME Tradng`). Candidates are blocked by first character and length (±3).

**Without a shared key:** `amount=` + `date=` (+ `days=3`).
- Rows with **equal values** whose dates are at most `days` apart are paired first. Within each value, both sides are walked in date order with two pointers, the classic greedy for pairing points on a line within a distance, which **maximises the number of pairs**. It's O(n log n) and has no candidate explosion when the same value recurs.
- With `tolerance > 0`, the **leftovers** are then paired with values within tolerance, closest date first.

**Two passes** (by reference, then by value + date for what's left):

```python
with ms.reconcile(dispatches, deliveries, on="tracking_no", amount="weight_kg") as r1:
    left_a, left_b = r1.only_in_a(), r1.only_in_b()   # original columns, ready to reuse
with ms.reconcile(left_a, left_b, amount="weight_kg", date="date", days=5) as r2:
    ...
```

**Inputs:** pandas, polars or pyarrow tables, a [docsift](https://github.com/mazin01/docsift) `Table`, or a `.csv` / `.parquet` path. CSV values are read as text and parsed by matchstream, so `"1,200"` and `14/03/2025` work. Dates can be ISO, day-first (`14/03/2025`, `14-03-2025`, `14.03.2025`) or use Arabic-Indic digits.

## CLI

```bash
matchstream orders.csv shipments.csv --on order_id:order --amount qty:units --out differences.csv
matchstream dispatches.csv deliveries.csv --amount weight_kg:weight --date date:received --days 3
```

It prints the summary as JSON. Errors are JSON with a stable code, and the exit status is 2.

## Benchmark

Synthetic data, single runs on a laptop (timings vary by roughly 2× between runs). Reproduce with `python bench/run.py`.

| scenario | rows (a × b) | matchstream | pandas `merge` | notes |
|---|---:|---:|---:|---|
| key match | 100,000 × 100,000 | 0.28 s | 0.13 s | same counts as pandas |
| key match | 1,000,000 × 1,000,000 | 1.05 s | 1.54 s | same counts as pandas; Arabic-safe normalization included |
| weight + date, no key | 100,000 × 100,000 | 0.32 s | n/a | 100% paired; true partner for 100% of unique weights, 80.3% overall |

- **The pandas baseline isn't a reconciliation.** It has no key normalization, it fans duplicate keys out into a cross product, and it has no keyless mode. It's here as the speed reference for "just join it".
- **Key normalization costs almost nothing.** Keys that are already printable ASCII, plain numbers and ISO dates are handled in SQL, so only the rest goes through the Python parsers. A test checks that both paths give identical results.
- **On the keyless 80.3%:** 20% of the generated rows are identical 99 kg pallets on different days. With no key, those can't be told apart, so any pairing of them is as good as another. Every unique weight found its true partner.
- **History:** the streaming pure-Python matcher this code replaces took 7.8 s for the 100k case. The same reconciliation as one DuckDB query takes 0.2 s.

## Errors

`matchstream.MatchError` carries `.code` and `.details`:

| code | when |
|---|---|
| `NOTHING_TO_MATCH_ON` | no `on`, and not both `amount` and `date` |
| `COLUMN_NOT_FOUND` | a named column is missing (`details` lists the available ones) |
| `BAD_TOLERANCE`, `BAD_FUZZY`, `BAD_ARGUMENT` | invalid parameters |
| `FILE_NOT_FOUND`, `UNSUPPORTED_FILE` | input path problems |

Unreadable values never crash a run. A value like `"n/a"` becomes NULL, and that pair is reported as `amount_mismatch`.

## Limits

- Pairing in fuzzy and tolerance passes is greedy (best candidate first, at most 5 candidates per row), not a globally optimal assignment. The equal-value pass is optimal in the number of pairs.
- Day-first dates are assumed: `03/04/2025` is 3 April.
- The full result stays inside DuckDB until you call `df()` or `to_csv` / `to_parquet`. `df()` materializes it in pandas.

## Use cases

- Orders vs. shipments, dispatches vs. deliveries, purchase orders vs. goods received.
- Stock counts vs. the inventory system, and asset registers vs. physical audits.
- Data migrations: the same records exported from the old and the new system (CRM, product catalogue, HR).
- Lists from different sources: registrations vs. attendance, survey invites vs. responses.
- Finance teams use the same pattern for statement vs. ledger checks.

## Development

```bash
pip install -e ".[test]"
pytest
python bench/run.py
```

## License

MIT
