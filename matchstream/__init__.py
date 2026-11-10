"""matchstream: reconcile two tables (by key, or by amount + date) on DuckDB.

    import matchstream as ms

    r = ms.reconcile("ledger.csv", "bank.csv", on="ref", amount="amount", tolerance=0.01)
    r.summary              # counts and totals per status
    r.df()                 # one row per pair or leftover, with status
    r.to_csv("out.csv")

Statuses: matched, matched_fuzzy, amount_mismatch, only_in_a, only_in_b.
"""
from __future__ import annotations

import math
import time
from datetime import datetime
from pathlib import Path

import duckdb
import nuqta as nq

__all__ = ["reconcile", "Result", "MatchError", "STATUSES"]
__version__ = "0.1.0"

STATUSES = ("matched", "matched_fuzzy", "amount_mismatch", "only_in_a", "only_in_b")
CANDIDATES_PER_ROW = 5          # fuzzy/window: best candidates kept per left row before pairing
FUZZY_MAX_LENGTH_GAP = 3        # fuzzy blocking: keys must share a first char and be ±3 long
_DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y",
                 "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S")
_NUMERIC = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER",
            "UBIGINT", "FLOAT", "DOUBLE", "REAL")


class MatchError(ValueError):
    """Bad input, with a stable machine-readable `code` (e.g. COLUMN_NOT_FOUND)."""

    def __init__(self, code: str, message: str, **details):
        super().__init__(f"{code}: {message}")
        self.code, self.details = code, details


# ── argument parsing ───────────────────────────────────────────────────────────

def _pair(x, what: str) -> tuple[str, str] | None:
    """"col" → ("col", "col"); ("left", "right") stays a pair."""
    if x is None:
        return None
    if isinstance(x, str):
        return (x, x)
    if isinstance(x, tuple) and len(x) == 2 and all(isinstance(v, str) for v in x):
        return x
    raise MatchError("BAD_ARGUMENT", f"{what} must be a column name or a (left, right) tuple", value=repr(x))


def _keys(on) -> list[tuple[str, str]]:
    """on="ref" | ("ref", "reference") | ["ref", "branch"] (a list means several keys)."""
    if on is None:
        return []
    if isinstance(on, list):
        if not on:
            raise MatchError("BAD_ARGUMENT", "on=[] has no keys")
        return [_pair(k, "each key in on") for k in on]
    return [_pair(on, "on")]


def _tolerance(t) -> tuple[str, float]:
    """0.01 → absolute; "1%" → percent of the left amount."""
    if isinstance(t, str) and t.strip().endswith("%"):
        value, kind = nq.to_number(t.strip()[:-1]), "percent"
    elif isinstance(t, (int, float)) and not isinstance(t, bool):
        value, kind = float(t), "absolute"
    else:
        value, kind = None, ""
    if value is None or not math.isfinite(value) or value < 0:
        raise MatchError("BAD_TOLERANCE", "tolerance must be a non-negative number or a percent like '1%'",
                         value=repr(t))
    return kind, value


# ── value parsing (registered as DuckDB functions) ─────────────────────────────

def _key(value):
    k = nq.normalize_key(value)
    return k or None  # empty keys never match each other


def _date(value):
    if value is None:
        return None
    s = nq.normalize_digits(value.strip())
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _register(con) -> None:
    # "special": the parsers return NULL for values they can't read (default mode forbids it).
    for name, fn, ret in (("ms_key", _key, "VARCHAR"), ("ms_num", nq.to_number, "DOUBLE"), ("ms_date", _date, "DATE")):
        con.create_function(name, fn, ["VARCHAR"], ret, null_handling="special")


def _lit(path: Path) -> str:
    """A path as a SQL string literal."""
    return "'" + path.as_posix().replace("'", "''") + "'"


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# ── loading ────────────────────────────────────────────────────────────────────

def _load(con, side: str, src) -> list[tuple[str, str]]:
    """Create table `side` with a 1-based __row; return [(column, type)] of the source."""
    if isinstance(src, (str, Path)):
        p = Path(src)
        if not p.is_file():
            raise MatchError("FILE_NOT_FOUND", str(p), side=side)
        ext = p.suffix.lower()
        if ext == ".parquet":
            scan = f"read_parquet({_lit(p)})"
        elif ext in (".csv", ".tsv", ".txt"):
            scan = f"read_csv({_lit(p)}, all_varchar = true)"  # values are parsed by ms_num/ms_date
        else:
            raise MatchError("UNSUPPORTED_FILE", f"{ext}: pass a DataFrame, a .csv or a .parquet", side=side)
    else:
        if hasattr(src, "to_pandas") and not hasattr(src, "to_arrow"):  # e.g. a docsift Table
            src = src.to_pandas()
        con.register(f"{side}_src", src)
        scan = f"{side}_src"
    # Insertion order is preserved by DuckDB's default settings (checked in the tests).
    con.execute(f"CREATE TEMP TABLE {side} AS SELECT row_number() OVER () AS __row, * FROM {scan}")
    return [(c, t) for c, t, *_ in con.execute(f"DESCRIBE {side}").fetchall() if c != "__row"]


def _check(cols: list[tuple[str, str]], wanted: list[str], side: str) -> None:
    names = [c for c, _ in cols]
    for w in wanted:
        if w not in names:
            raise MatchError("COLUMN_NOT_FOUND", f"column {w!r} not in table {side}", side=side,
                             column=w, available=names)


def _expr(col: str, cols: dict, kind: str) -> str:
    c = _q(col)
    if kind == "amount":
        if cols[col].split("(")[0] in _NUMERIC or cols[col].startswith("DECIMAL"):
            return f"CAST({c} AS DOUBLE)"
        # Plain numbers skip the Python parser; "1,250.00", "(5)", Arabic digits don't.
        v = f"CAST({c} AS VARCHAR)"
        return f"CASE WHEN regexp_full_match({v}, '-?[0-9]+(\\.[0-9]+)?') THEN CAST({v} AS DOUBLE) ELSE ms_num({v}) END"
    if kind == "date":
        if cols[col] in ("DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE"):
            return f"CAST({c} AS DATE)"
        v = f"CAST({c} AS VARCHAR)"
        return f"CASE WHEN regexp_full_match({v}, '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}') THEN TRY_CAST({v} AS DATE) " \
               f"ELSE ms_date({v}) END"
    raise AssertionError(kind)


def _key_expr(col: str, normalize: bool) -> str:
    v = f"CAST({_q(col)} AS VARCHAR)"
    if not normalize:
        return f"nullif(trim({v}), '')"
    # Printable ASCII without spaces is already normalized: skip the Python call.
    return (f"CASE WHEN regexp_full_match({v}, '[!-~]+') AND lower({v}) NOT IN ('nan', 'none', 'nat', 'null') "
            f"THEN {v} ELSE ms_key({v}) END")


# ── pairing ────────────────────────────────────────────────────────────────────

def _greedy(candidates) -> list[tuple[int, int]]:
    """One-to-one pairs from candidates already sorted best-first.

    ponytail: greedy, not an optimal assignment; fine when candidates are
    selective (keys, amounts + dates). Swap in scipy's linear_sum_assignment if
    you need the global optimum on dense ties.
    """
    used_a, used_b, out = set(), set(), []
    for ra, rb, *_ in candidates:
        if ra not in used_a and rb not in used_b:
            used_a.add(ra)
            used_b.add(rb)
            out.append((ra, rb))
    return out


def _same_amount_pairs(con, days: int) -> list[tuple[int, int]]:
    """Rows with the same amount and dates at most `days` apart.

    Walk both sides sorted by (amount, date) with two pointers: within one amount
    this is the classic greedy for pairing points on a line within a distance,
    which pairs the maximum possible number of rows. O(n log n), no candidate join.
    """
    q = "SELECT __amt, __d, __row FROM {}_x WHERE __amt IS NOT NULL AND __d IS NOT NULL ORDER BY __amt, __d, __row"
    ra, rb = con.execute(q.format("a")).fetchall(), con.execute(q.format("b")).fetchall()
    pairs, i, j = [], 0, 0
    while i < len(ra) and j < len(rb):
        (amt_a, d_a, row_a), (amt_b, d_b, row_b) = ra[i], rb[j]
        if amt_a != amt_b:
            i, j = (i + 1, j) if amt_a < amt_b else (i, j + 1)
            continue
        gap = (d_b - d_a).days
        if abs(gap) <= days:
            pairs.append((row_a, row_b))
            i, j = i + 1, j + 1
        elif gap > 0:
            i += 1  # this a-row is too early for any remaining b-row of the same amount
        else:
            j += 1
    return pairs


def _insert(con, pairs, how: str, similarity=None) -> None:
    """Bulk-insert (row_a, row_b) pairs; executemany is slow in DuckDB."""
    if not pairs:
        return
    import pandas as pd

    df = pd.DataFrame(pairs, columns=["row_a", "row_b"])
    df["how"], df["similarity"] = how, similarity
    con.register("_new_pairs", df)
    con.execute("INSERT INTO pairs SELECT row_a, row_b, how, similarity FROM _new_pairs")
    con.unregister("_new_pairs")


def reconcile(a, b, *, on=None, amount=None, tolerance=0, date=None, days: int = 3,
              fuzzy: float | None = None, normalize: bool = True) -> "Result":
    """Reconcile table `a` against table `b`.

    a, b       DataFrame (pandas, polars, pyarrow), docsift Table, or a .csv/.parquet path.
    on         key column(s): "ref", ("ref", "reference") for different names, or a
               list for a composite key. Duplicate keys are paired one-to-one,
               smallest amounts first, never as a cross product.
    amount     amount column(s), compared within `tolerance` (0.01 or "1%").
               Text amounts are parsed: "١٬٢٥٠٫٥٠", "(1,250.00)", "1,250.00".
    date, days Without `on`: match rows whose amounts agree and whose dates are at
               most `days` apart. Equal amounts are paired first (as many pairs as
               possible, in date order); then leftovers within `tolerance`.
    fuzzy      With a single key: pair leftover keys whose Jaro-Winkler
               similarity is at least this (e.g. 0.9) as matched_fuzzy.
    normalize  Compare keys after nuqta.normalize_key (Arabic digits, invisible
               characters, spacing). Default True.
    """
    started = time.perf_counter()
    keys, amt, dt = _keys(on), _pair(amount, "amount"), _pair(date, "date")
    tol_kind, tol = _tolerance(tolerance)
    if not keys and not (amt and dt):
        raise MatchError("NOTHING_TO_MATCH_ON", "pass on=... (keys), or amount=... and date=... (no shared key)")
    if fuzzy is not None and (len(keys) != 1 or not 0 < fuzzy <= 1):
        raise MatchError("BAD_FUZZY", "fuzzy needs exactly one key column and a threshold in (0, 1]",
                         value=repr(fuzzy))
    if not isinstance(days, int) or days < 0:
        raise MatchError("BAD_ARGUMENT", "days must be a non-negative integer", value=repr(days))

    con = duckdb.connect()
    try:
        _register(con)
        cols = {}
        for side, src, idx in (("a", a, 0), ("b", b, 1)):
            c = _load(con, side, src)
            _check(c, [k[idx] for k in keys] + [p[idx] for p in (amt, dt) if p], side)
            cols[side] = dict(c)

        for side, idx in (("a", 0), ("b", 1)):
            parts = [_key_expr(k[idx], normalize) for k in keys]
            key = "NULL" if not parts else parts[0] if len(parts) == 1 else \
                "CASE WHEN " + " AND ".join(f"{p} IS NOT NULL" for p in parts) + \
                " THEN concat_ws(chr(31), " + ", ".join(parts) + ") END"
            am = _expr(amt[idx], cols[side], "amount") if amt else "NULL::DOUBLE"
            da = _expr(dt[idx], cols[side], "date") if dt else "NULL::DATE"
            con.execute(f"CREATE TEMP TABLE {side}_x AS SELECT __row, {key} AS __k, {am} AS __amt, "
                        f"{da} AS __d FROM {side}")

        within = "abs(b.__amt - a.__amt) <= " + (f"{tol}" if tol_kind == "absolute" else f"{tol / 100} * abs(a.__amt)")
        con.execute("CREATE TEMP TABLE pairs (row_a BIGINT, row_b BIGINT, how VARCHAR, similarity DOUBLE)")

        if keys:
            # Duplicate keys pair 1:1 in amount order: the n-th smallest with the n-th smallest.
            for side in ("a", "b"):
                con.execute(f"CREATE TEMP TABLE {side}_n AS SELECT __row, __k, row_number() OVER "
                            f"(PARTITION BY __k ORDER BY __amt NULLS LAST, __row) AS __n FROM {side}_x "
                            f"WHERE __k IS NOT NULL")
            con.execute("INSERT INTO pairs SELECT a.__row, b.__row, 'key', NULL FROM a_n a "
                        "JOIN b_n b ON a.__k = b.__k AND a.__n = b.__n")
            if fuzzy is not None:
                cand = con.execute(f"""
                    WITH la AS (SELECT x.* FROM a_x x ANTI JOIN pairs p ON x.__row = p.row_a WHERE x.__k IS NOT NULL),
                         lb AS (SELECT x.* FROM b_x x ANTI JOIN pairs p ON x.__row = p.row_b WHERE x.__k IS NOT NULL),
                         c AS (SELECT a.__row ra, b.__row rb, jaro_winkler_similarity(a.__k, b.__k) s
                               FROM la a JOIN lb b ON left(a.__k, 1) = left(b.__k, 1)
                               AND abs(length(a.__k) - length(b.__k)) <= {FUZZY_MAX_LENGTH_GAP})
                    SELECT ra, rb, s FROM c WHERE s >= {fuzzy}
                    QUALIFY row_number() OVER (PARTITION BY ra ORDER BY s DESC, rb) <= {CANDIDATES_PER_ROW}
                    ORDER BY s DESC, ra, rb""").fetchall()
                sim = {(ra, rb): s for ra, rb, s in cand}
                chosen = _greedy(cand)
                _insert(con, chosen, "fuzzy", [sim[p] for p in chosen])
        else:
            _insert(con, _same_amount_pairs(con, days), "window")
            if tol > 0:  # amounts that differ, but within tolerance: only rows left over
                cand = con.execute(f"""
                    WITH a AS (SELECT x.* FROM a_x x ANTI JOIN pairs p ON x.__row = p.row_a),
                         b AS (SELECT x.* FROM b_x x ANTI JOIN pairs p ON x.__row = p.row_b)
                    SELECT a.__row ra, b.__row rb FROM a JOIN b
                      ON b.__d BETWEEN a.__d - INTERVAL {days} DAY AND a.__d + INTERVAL {days} DAY AND {within}
                    QUALIFY row_number() OVER (PARTITION BY a.__row ORDER BY abs(date_diff('day', a.__d, b.__d)),
                            abs(b.__amt - a.__amt), b.__row) <= {CANDIDATES_PER_ROW}
                    ORDER BY abs(date_diff('day', a.__d, b.__d)), abs(b.__amt - a.__amt), ra, rb""").fetchall()
                _insert(con, _greedy(cand), "window")

        con.execute("INSERT INTO pairs SELECT x.__row, NULL, NULL, NULL FROM a_x x ANTI JOIN pairs p ON x.__row = p.row_a")
        con.execute("INSERT INTO pairs SELECT NULL, x.__row, NULL, NULL FROM b_x x ANTI JOIN pairs p ON x.__row = p.row_b")

        mismatch = f"WHEN a.__amt IS NULL OR b.__amt IS NULL OR NOT ({within}) THEN 'amount_mismatch' " if amt else ""
        extra = []
        if amt:
            extra.append("b.__amt - a.__amt AS amount_diff")
        if dt:
            extra.append("date_diff('day', a.__d, b.__d) AS days_apart")
        if fuzzy is not None:
            extra.append("p.similarity")
        a_cols = [f'ra.{_q(c)} AS {_q(c + "_a")}' for c in cols["a"]]
        b_cols = [f'rb.{_q(c)} AS {_q(c + "_b")}' for c in cols["b"]]
        con.execute(f"""
            CREATE TEMP TABLE result AS
            SELECT CASE WHEN p.row_b IS NULL THEN 'only_in_a' WHEN p.row_a IS NULL THEN 'only_in_b'
                        {mismatch}WHEN p.how = 'fuzzy' THEN 'matched_fuzzy' ELSE 'matched' END AS status,
                   p.row_a, p.row_b, {", ".join(extra + a_cols + b_cols)}
            FROM pairs p
            LEFT JOIN a_x a ON a.__row = p.row_a LEFT JOIN b_x b ON b.__row = p.row_b
            LEFT JOIN a ra ON ra.__row = p.row_a LEFT JOIN b rb ON rb.__row = p.row_b
            ORDER BY coalesce(p.row_a, 9223372036854775807), p.row_b""")

        counts = dict(con.execute("SELECT status, count(*) FROM result GROUP BY 1").fetchall())
        summary = {"mode": "key" if keys else "amount+date",
                   "rows_a": con.execute("SELECT count(*) FROM a").fetchone()[0],
                   "rows_b": con.execute("SELECT count(*) FROM b").fetchone()[0],
                   "counts": {s: counts.get(s, 0) for s in STATUSES if s != "matched_fuzzy" or fuzzy is not None}}
        if amt:
            totals = con.execute("""SELECT status, round(sum(a.__amt), 2), round(sum(b.__amt), 2) FROM result r
                                    LEFT JOIN a_x a ON a.__row = r.row_a LEFT JOIN b_x b ON b.__row = r.row_b
                                    GROUP BY 1""").fetchall()
            by_status = {s: {"a": ta, "b": tb} for s, ta, tb in totals}
            summary["totals"] = {s: by_status[s] for s in STATUSES if s in by_status}
        summary["seconds"] = round(time.perf_counter() - started, 3)
        return Result(con, cols, summary)
    except BaseException:
        con.close()
        raise


class Result:
    """Reconciliation output, held in DuckDB until you ask for it."""

    def __init__(self, con, cols: dict, summary: dict):
        self._con, self._cols, self.summary = con, cols, summary

    def df(self, status: str | None = None):
        """pandas DataFrame of the result, optionally one status only."""
        if status is not None and status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}")
        where = "" if status is None else f" WHERE status = '{status}'"
        return self._con.execute(f"SELECT * FROM result{where}").df()

    def _left_over(self, side: str):
        names = ", ".join(f"{_q(c + '_' + side)} AS {_q(c)}" for c in self._cols[side])
        return self._con.execute(f"SELECT {names} FROM result WHERE status = 'only_in_{side}' "
                                 f"ORDER BY row_{side}").df()

    def only_in_a(self):
        """Unmatched rows of `a` with their original columns: feed them to a second pass."""
        return self._left_over("a")

    def only_in_b(self):
        return self._left_over("b")

    def to_csv(self, path) -> None:
        self._con.execute(f"COPY result TO {_lit(Path(path))} (HEADER, DELIMITER ',')")

    def to_parquet(self, path) -> None:
        self._con.execute(f"COPY result TO {_lit(Path(path))} (FORMAT parquet)")

    def close(self) -> None:
        self._con.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __repr__(self) -> str:
        return f"<matchstream.Result {self.summary['counts']}>"
