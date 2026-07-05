import pandas as pd

from pipeline import config
from pipeline.crosscheck import CrossCheckResult, compare_sources


def _canon_frame(rows: list[tuple[str, float]]) -> pd.DataFrame:
    """A minimal CANONICAL-shaped frame (config.CANON_COLUMNS) carrying just
    the (instrument_key, close) pairs a test needs -- every other column is
    filled with an inert placeholder since compare_sources only reads
    instrument_key/close."""
    data = {c: [] for c in config.CANON_COLUMNS}
    for key, close in rows:
        data["date"].append(pd.Timestamp("2026-07-03"))
        data["instrument_key"].append(key)
        data["isin"].append(key)
        data["symbol"].append(key)
        data["series"].append("EQ")
        data["open"].append(close)
        data["high"].append(close)
        data["low"].append(close)
        data["close"].append(close)
        data["prevclose"].append(close)
        data["volume"].append(1)
        data["value"].append(1.0)
        data["trades"].append(1)
        data["source"].append("test")
    return pd.DataFrame(data)[config.CANON_COLUMNS]


def test_all_agree_zero_mismatches():
    primary = _canon_frame([("A", 100.0), ("B", 200.0), ("C", 300.0)])
    secondary = _canon_frame([("A", 100.0), ("B", 200.0), ("C", 300.0)])
    result = compare_sources(primary, secondary)
    assert result == CrossCheckResult(compared=3, mismatched=0, worst=[])


def test_single_mismatch_is_counted_and_appears_in_worst():
    primary = _canon_frame([("A", 100.0), ("B", 200.0), ("C", 300.0)])
    # B diverges by 10% -- far outside the default 0.001 tolerance.
    secondary = _canon_frame([("A", 100.0), ("B", 220.0), ("C", 300.0)])
    result = compare_sources(primary, secondary)
    assert result.compared == 3
    assert result.mismatched == 1
    assert result.worst == [("B", 200.0, 220.0)]


def test_tolerance_boundary_exactly_at_is_not_a_mismatch():
    # primary=2.0, secondary=3.0 -> relative divergence = |2-3|/2 = 0.5,
    # exactly representable in binary float (no rounding noise) -- set
    # tolerance to that SAME exact value so this genuinely proves the ">"
    # (not ">=") comparison: AT tolerance must NOT count as a mismatch.
    primary = _canon_frame([("A", 2.0)])
    secondary = _canon_frame([("A", 3.0)])
    result = compare_sources(primary, secondary, tolerance=0.5)
    assert result.mismatched == 0


def test_tolerance_boundary_just_over_is_a_mismatch():
    # Same exact-float pair (divergence = 0.5), tolerance set just BELOW it
    # -- must count as a mismatch.
    primary = _canon_frame([("A", 2.0)])
    secondary = _canon_frame([("A", 3.0)])
    result = compare_sources(primary, secondary, tolerance=0.499)
    assert result.mismatched == 1


def test_deterministic_sampling_same_input_twice_identical_result():
    keys = [(f"SYM{i:03d}", 100.0 + i) for i in range(200)]
    primary = _canon_frame(keys)
    secondary = _canon_frame(keys)
    result_a = compare_sources(primary, secondary, sample_n=50)
    result_b = compare_sources(primary, secondary, sample_n=50)
    assert result_a == result_b


def test_sample_n_larger_than_population_compares_everything():
    keys = [(f"SYM{i:02d}", 100.0 + i) for i in range(10)]
    primary = _canon_frame(keys)
    secondary = _canon_frame(keys)
    result = compare_sources(primary, secondary, sample_n=50)
    assert result.compared == 10


def test_worst_caps_at_five_sorted_by_relative_divergence_descending():
    # 6 mismatches, each with a distinct relative divergence -- only the
    # worst 5 (by relative divergence, descending) survive into `worst`.
    keys = [(f"SYM{i}", 100.0) for i in range(6)]
    primary = _canon_frame(keys)
    # Divergences: SYM0=1%, SYM1=2%, ..., SYM5=6% -- all well past tolerance.
    secondary = _canon_frame([(f"SYM{i}", 100.0 * (1 + 0.01 * (i + 1))) for i in range(6)])
    result = compare_sources(primary, secondary, sample_n=50, tolerance=0.001)
    assert result.mismatched == 6
    assert len(result.worst) == 5
    worst_keys = [w[0] for w in result.worst]
    # SYM5 (6% divergence) is worst, SYM0 (1%) is the smallest of the survivors.
    assert worst_keys == ["SYM5", "SYM4", "SYM3", "SYM2", "SYM1"]


def test_keys_only_in_primary_are_excluded_by_inner_join():
    primary = _canon_frame([("A", 100.0), ("ONLY_PRIMARY", 50.0)])
    secondary = _canon_frame([("A", 100.0), ("ONLY_SECONDARY", 75.0)])
    result = compare_sources(primary, secondary)
    assert result.compared == 1  # only "A" is in both


def test_empty_intersection_returns_zero_compared_zero_mismatched():
    primary = _canon_frame([("ONLY_PRIMARY", 50.0)])
    secondary = _canon_frame([("ONLY_SECONDARY", 75.0)])
    result = compare_sources(primary, secondary)
    assert result == CrossCheckResult(compared=0, mismatched=0, worst=[])


def test_seed_symbols_are_always_included_in_the_compared_sample():
    # 100 symbols, sample_n=10 -> stride=10 -> the deterministic stride alone
    # would land on SYM000, SYM010, SYM020, ... and skip SYM005 entirely.
    # Passing it as a seed must force it into the compared sample anyway.
    keys = [(f"SYM{i:03d}", 100.0 + i) for i in range(100)]
    primary = _canon_frame(keys)
    secondary = _canon_frame(keys)
    result = compare_sources(primary, secondary, sample_n=10, seed_symbols=["SYM005"])
    assert result.compared == 11  # the usual 10-symbol stride + 1 forced seed


def test_seed_symbols_none_is_a_no_op():
    keys = [(f"SYM{i:03d}", 100.0 + i) for i in range(100)]
    primary = _canon_frame(keys)
    secondary = _canon_frame(keys)
    without_seed = compare_sources(primary, secondary, sample_n=10)
    with_none_seed = compare_sources(primary, secondary, sample_n=10, seed_symbols=None)
    assert without_seed == with_none_seed
