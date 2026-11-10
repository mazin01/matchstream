import pandas as pd
import pytest

import matchstream as ms
from matchstream import MatchError


def run(a, b, **kw):
    with ms.reconcile(pd.DataFrame(a), pd.DataFrame(b), **kw) as r:
        return r.summary, r.df()


# ── keyed ──────────────────────────────────────────────────────────────────────

def test_statuses_and_amount_tolerance():
    a = {"ref": ["A1", "A2", "A3", "A4"], "amount": [100.0, 200.0, 300.0, 400.0]}
    b = {"ref": ["A1", "A2", "A3", "B9"], "amount": [100.004, 205.0, 300.0, 50.0]}
    summary, df = run(a, b, on="ref", amount="amount", tolerance=0.01)
    assert summary["counts"] == {"matched": 2, "amount_mismatch": 1, "only_in_a": 1, "only_in_b": 1}
    row = df[df.status == "amount_mismatch"].iloc[0]
    assert row.ref_a == "A2" and row.amount_diff == pytest.approx(5.0)
    assert summary["totals"]["only_in_b"]["b"] == 50.0


def test_percent_tolerance():
    a, b = {"ref": ["x", "y"], "amount": [1000.0, 1000.0]}, {"ref": ["x", "y"], "amount": [1009.0, 1011.0]}
    summary, _ = run(a, b, on="ref", amount="amount", tolerance="1%")
    assert summary["counts"]["matched"] == 1 and summary["counts"]["amount_mismatch"] == 1


def test_arabic_keys_and_amounts_are_normalized():
    a = {"رقم": ["INV-١٢٣", "‏INV-124‏"], "المبلغ": ["١٬٢٥٠٫٥٠", "(100.00)"]}
    b = {"ref": ["INV-123", "INV-124"], "amt": [1250.5, -100.0]}
    summary, _ = run(a, b, on=("رقم", "ref"), amount=("المبلغ", "amt"))
    assert summary["counts"]["matched"] == 2


def test_normalize_off_compares_raw_keys():
    summary, _ = run({"k": ["INV-١٢٣"]}, {"k": ["INV-123"]}, on="k", normalize=False)
    assert summary["counts"]["only_in_a"] == 1


def test_duplicate_keys_pair_one_to_one_by_amount():
    a = {"ref": ["R", "R", "R"], "amount": [200.0, 100.0, 300.0]}
    b = {"ref": ["R", "R"], "amount": [100.0, 200.0]}
    summary, df = run(a, b, on="ref", amount="amount")
    assert summary["counts"]["matched"] == 2 and summary["counts"]["only_in_a"] == 1
    assert df[df.status == "only_in_a"].amount_a.tolist() == [300.0]  # no cross product


def test_composite_key_and_empty_keys_never_match():
    a = {"ref": ["1", "1", None], "branch": ["N", "S", "N"]}
    b = {"ref": ["1", "1", None], "branch": ["S", "E", "N"]}
    summary, _ = run(a, b, on=["ref", "branch"])
    assert summary["counts"] == {"matched": 1, "amount_mismatch": 0, "only_in_a": 2, "only_in_b": 2}


def test_fuzzy_pairs_leftover_keys_once():
    a = {"name": ["ACME Trading", "Gulf Supplies", "Zed Ltd"]}
    b = {"name": ["ACME Tradng", "ACME Tradin", "Gulf Suplies"]}
    summary, df = run(a, b, on="name", fuzzy=0.9)
    assert summary["counts"]["matched_fuzzy"] == 2
    fz = df[df.status == "matched_fuzzy"].set_index("name_a")
    assert fz.loc["Gulf Supplies", "name_b"] == "Gulf Suplies"
    assert summary["counts"]["only_in_a"] == 1 and summary["counts"]["only_in_b"] == 1  # each b used once


# ── no shared key: amount + date window ────────────────────────────────────────

def test_amount_and_date_window_picks_closest_date_one_to_one():
    bank = {"date": ["2025-03-01", "2025-03-05", "2025-03-20"], "amount": ["100.00", "100.00", "75.50"]}
    ledger = {"posted": ["04/03/2025", "02/03/2025", "01/03/2025"], "debit": [100.0, 100.0, 80.0]}
    summary, df = run(bank, ledger, amount=("amount", "debit"), date=("date", "posted"), days=3)
    assert summary["mode"] == "amount+date"
    m = df[df.status == "matched"].sort_values("row_a")
    assert m.posted_b.tolist() == ["02/03/2025", "04/03/2025"]  # 1 Mar ↔ 2 Mar, 5 Mar ↔ 4 Mar
    assert m.days_apart.tolist() == [1, -1]
    assert summary["counts"]["only_in_a"] == 1 and summary["counts"]["only_in_b"] == 1


def test_second_pass_on_leftovers():
    a = {"ref": ["A1", None], "amount": [10.0, 99.0], "date": ["2025-01-01", "2025-01-10"]}
    b = {"ref": ["A1", "zzz"], "amount": [10.0, 99.0], "date": ["2025-01-01", "2025-01-11"]}
    with ms.reconcile(pd.DataFrame(a), pd.DataFrame(b), on="ref", amount="amount") as r1:
        left_a, left_b = r1.only_in_a(), r1.only_in_b()
    assert list(left_a.columns) == ["ref", "amount", "date"]
    with ms.reconcile(left_a, left_b, amount="amount", date="date") as r2:
        assert r2.summary["counts"]["matched"] == 1


# ── files, order, errors ───────────────────────────────────────────────────────

def test_csv_and_parquet_inputs_and_outputs(tmp_path):
    pd.DataFrame({"ref": ["a", "b"], "amount": ["1,000.00", "5"]}).to_csv(tmp_path / "a.csv", index=False)
    pd.DataFrame({"ref": ["a", "c"], "amount": [1000.0, 7.0]}).to_parquet(tmp_path / "b's.parquet")
    with ms.reconcile(tmp_path / "a.csv", tmp_path / "b's.parquet", on="ref", amount="amount") as r:
        r.to_csv(tmp_path / "out.csv")
        r.to_parquet(tmp_path / "out.parquet")
        assert r.summary["counts"]["matched"] == 1
    assert len(pd.read_csv(tmp_path / "out.csv")) == 3
    assert len(pd.read_parquet(tmp_path / "out.parquet")) == 3


def test_row_numbers_follow_input_order():
    n = 50_000
    a = pd.DataFrame({"ref": [f"k{i}" for i in range(n)][::-1]})
    with ms.reconcile(a, a.copy(), on="ref") as r:
        df = r.df()
    assert (df.ref_a.values == a.ref.values).all() and (df.row_a.values == range(1, n + 1)).all()


@pytest.mark.parametrize("kw,code", [
    ({}, "NOTHING_TO_MATCH_ON"),
    ({"on": "nope"}, "COLUMN_NOT_FOUND"),
    ({"on": "ref", "tolerance": -1}, "BAD_TOLERANCE"),
    ({"on": "ref", "tolerance": "lots"}, "BAD_TOLERANCE"),
    ({"on": ["ref", "ref"], "fuzzy": 0.9}, "BAD_FUZZY"),
    ({"on": 5}, "BAD_ARGUMENT"),
])
def test_errors_have_stable_codes(kw, code):
    with pytest.raises(MatchError) as e:
        ms.reconcile(pd.DataFrame({"ref": ["a"]}), pd.DataFrame({"ref": ["a"]}), **kw)
    assert e.value.code == code


def test_missing_file_and_bad_status(tmp_path):
    with pytest.raises(MatchError) as e:
        ms.reconcile(tmp_path / "nope.csv", pd.DataFrame({"ref": []}), on="ref")
    assert e.value.code == "FILE_NOT_FOUND"
    with ms.reconcile(pd.DataFrame({"ref": ["a"]}), pd.DataFrame({"ref": ["a"]}), on="ref") as r:
        with pytest.raises(ValueError):
            r.df("weird")


def test_sql_fast_paths_agree_with_python_parsers():
    import duckdb

    from matchstream import _date, _expr, _key, _key_expr, _register

    values = ["ABC-1", "abc def", " padded ", "NaN", "null", "None", "INV-١٢٣", "‏X‏", "x y",
              "", "-12.5", "1,250.00", "(5)", "١٢٣", "2025-03-14", "14/03/2025", "2025-02-30", "12a"]
    con = duckdb.connect()
    _register(con)
    con.execute("CREATE TABLE t (v VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?)", [(v,) for v in values])
    cols = {"v": "VARCHAR"}
    got = con.execute(f"SELECT {_key_expr('v', True)}, {_expr('v', cols, 'amount')}, "
                      f"{_expr('v', cols, 'date')} FROM t").fetchall()
    con.close()
    for v, (k, n, d) in zip(values, got):
        assert k == _key(v), v
        assert n == ms.nq.to_number(v), v
        assert d == _date(v), v


def test_unreadable_values_do_not_crash():
    a = {"ref": ["A", "", "C"], "amount": ["n/a", "5", "7"], "date": ["soon", "2025-01-01", "2025-01-02"]}
    b = {"ref": ["A", "", "C"], "amount": ["10", "5", "7"], "date": ["2025-01-01", "2025-01-01", "2025-01-02"]}
    summary, df = run(a, b, on="ref", amount="amount")
    assert summary["counts"] == {"matched": 1, "amount_mismatch": 1, "only_in_a": 1, "only_in_b": 1}
    summary, _ = run(a, b, amount="amount", date="date")
    assert summary["counts"]["matched"] == 2
