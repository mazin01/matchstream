# matchstream

**Reconcile two tables in one call: by key, or by amount + date when there's no shared key. Built on DuckDB, with Arabic-safe keys and amounts.**

Reconciliation is a join plus the details that make plain joins wrong:

- **Keys that look equal but aren't:** `INV-١٢٣` vs `INV-123`, or a header wrapped in invisible RTL marks by Excel.
- **Amounts stored as text:** `"1,250.00"`, `"(100.00)"`, `"١٬٢٥٠٫٥٠"`.
- **Duplicate keys:** two payments with the same reference turn a plain join into a cross product.
- **No shared key at all:** a bank statement vs. a ledger, where only the amount and roughly the date agree.

```python
import matchstream as ms

with ms.reconcile("ledger.csv", "bank.parquet", on=("ref", "reference"),
                  amount=("amount", "credit"), tolerance=0.01) as r:
    print(r.summary)        # counts and totals per status
    r.df("amount_mismatch") # pandas DataFrame
    r.to_csv("result.csv")
```

```python
{'mode': 'key', 'rows_a': 3, 'rows_b': 3,
 'counts': {'matched': 1, 'amount_mismatch': 1, 'only_in_a': 1, 'only_in_b': 1},
 'totals': {'matched': {'a': 1250.0, 'b': 1250.0}, 'amount_mismatch': {'a': 99.0, 'b': 98.0}, ...},
 'seconds': 0.543}
```

Every row of both tables appears exactly once in the result, with a `status`, its original row numbers (`row_a`, `row_b`), the differences (`amount_diff`, `days_apart`, `similarity`), and all original columns suffixed `_a` / `_b`.

| status | meaning |
|---|---|
| `matched` | paired, and the amounts agree within tolerance (or no amount was given) |
| `matched_fuzzy` | paired by a similar (not equal) key: `fuzzy=` |
| `amount_mismatch` | paired by key, but the amounts differ |
| `only_in_a` / `only_in_b` | no partner |

## Install

```bash
pip install "matchstream @ git+https://github.com/mazin01/matchstream"
```

Dependencies: `duckdb`, `pandas` and [`nuqta`](https://github.com/mazin01/nuqta) (Arabic text helpers). Add `pyarrow` to read or write Parquet from pandas.

## Matching modes

**By key.** `on="ref"`, or `on=("ref", "reference")` when the names differ, or `on=["ref", "branch"]` for a composite key (a list means several keys).
- Keys are compared after [`nuqta.normalize_key`](https://github.com/mazin01/nuqta): Arabic-Indic digits, invisible bidi marks, NBSPs, tatweel and spacing. Turn it off with `normalize=False`.
- Empty or null-like keys (`""`, `NaN`, `None`) never match each other.
- **Duplicate keys pair one-to-one**, the n-th smallest amount with the n-th smallest. Three payments against two ledger lines give two pairs and one `only_in_a`, never six rows.
- `amount=` with `tolerance=0.01` (absolute) or `tolerance="1%"` (of the left amount) splits pairs into `matched` and `amount_mismatch`.
- `fuzzy=0.9` gives a second pass: leftover keys with Jaro-Winkler similarity ≥ 0.9 are paired one-to-one, best first (`ACME Trading` ↔ `ACME Tradng`). Candidates are blocked by first character and length (±3).

**Without a shared key:** `amount=` + `date=` (+ `days=3`).
- Rows with **equal amounts** whose dates are at most `days` apart are paired first. Within each amount, both sides are walked in date order with two pointers, the classic greedy for pairing points on a line within a distance, which **maximises the number of pairs**. It's O(n log n) and has no candidate explosion when the same amount recurs.
- With `tolerance > 0`, the **leftovers** are then paired with amounts within tolerance, closest date first.

**Two passes** (by reference, then by amount + date for what's left):

```python
with ms.reconcile(ledger, bank, on="ref", amount="amount") as r1:
    left_a, left_b = r1.only_in_a(), r1.only_in_b()   # original columns, ready to reuse
with ms.reconcile(left_a, left_b, amount="amount", date="date", days=5) as r2:
    ...
```

**Inputs:** pandas, polars or pyarrow tables, a [docsift](https://github.com/mazin01/docsift) `Table`, or a `.csv` / `.parquet` path. CSV values are read as text and parsed by matchstream, so `"1,250.00"` and `14/03/2025` work. Dates can be ISO, day-first (`14/03/2025`, `14-03-2025`, `14.03.2025`) or use Arabic-Indic digits.

## CLI

```bash
matchstream ledger.csv bank.csv --on ref:reference --amount amount:credit --tolerance 0.01 --out result.csv
matchstream bank.csv ledger.csv --amount amount:debit --date date:posted --days 3
```

It prints the summary as JSON. Errors are JSON with a stable code, and the exit status is 2.

## Benchmark

Synthetic data, single runs on a laptop (timings vary by roughly 2× between runs). Reproduce with `python bench/run.py`.

| scenario | rows (a × b) | matchstream | pandas `merge` | notes |
|---|---:|---:|---:|---|
| key match | 100,000 × 100,000 | 0.52 s | 0.22 s | same counts as pandas |
| key match | 1,000,000 × 1,000,000 | 2.65 s | 4.97 s | same counts as pandas; Arabic-safe normalization included |
| amount + date, no key | 100,000 × 100,000 | 1.22 s | n/a | 100% paired; true partner for 100% of unique amounts, 80.3% overall |

- **The pandas baseline isn't a reconcile.** It has no key normalization, it fans duplicate keys out into a cross product, and it has no keyless mode. It's here as the speed reference for "just join it".
- **Key normalization costs almost nothing.** Keys that are already printable ASCII, plain numbers and ISO dates are handled in SQL, so only the rest goes through the Python parsers. A test checks that both paths give identical results.
- **On the keyless 80.3%:** 20% of the generated rows are the same recurring 99.00 payment on different days. With no key, those can't be told apart, so any pairing of them is as good as another. Every unique amount found its true partner.
- **History:** the streaming pure-Python reconciler this code replaces took 7.8 s for the 100k case. The same reconcile as one DuckDB query takes 0.2 s.

## Errors

`matchstream.MatchError` carries `.code` and `.details`:

| code | when |
|---|---|
| `NOTHING_TO_MATCH_ON` | no `on`, and not both `amount` and `date` |
| `COLUMN_NOT_FOUND` | a named column is missing (`details` lists the available ones) |
| `BAD_TOLERANCE`, `BAD_FUZZY`, `BAD_ARGUMENT` | invalid parameters |
| `FILE_NOT_FOUND`, `UNSUPPORTED_FILE` | input path problems |

Unreadable values never crash a run. An amount like `"n/a"` becomes NULL, and that pair is reported as `amount_mismatch`.

## Limits

- Pairing in fuzzy and tolerance passes is greedy (best candidate first, at most 5 candidates per row), not a globally optimal assignment. The equal-amount pass is optimal in the number of pairs.
- Day-first dates are assumed: `03/04/2025` is 3 April.
- The full result stays inside DuckDB until you call `df()` or `to_csv` / `to_parquet`. `df()` materializes it in pandas.

## Use cases

- Bank statement vs. ledger, invoices vs. payments, and supplier statements vs. purchase records.
- Inventory counts vs. system stock, and order exports vs. shipping manifests.
- Any "what's in A but not in B" check where the keys come from different systems, including Arabic ones.

## Development

```bash
pip install -e ".[test]"
pytest
python bench/run.py
```

## License

MIT
