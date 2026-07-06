import pandas as pd
import pytest

from pipeline import config
from pipeline.errors import UnexpectedFailure
from pipeline.validate import check_rowcount_by_series, quarantine_bad_rows


def _row(**over) -> dict:
    base = {
        "date": pd.Timestamp("2026-07-03"),
        "instrument_key": "INE002A01018", "isin": "INE002A01018",
        "symbol": "RELIANCE", "series": "EQ",
        "open": 2990.0, "high": 3010.0, "low": 2985.0, "close": 3000.0,
        "prevclose": 2980.0, "volume": 1000, "value": 1.0, "trades": 10,
        "source": "nse-udiff",
    }
    base.update(over)
    return base


def test_quarantine_separates_bad_rows():
    df = pd.DataFrame([
        _row(symbol="GOOD"),
        _row(symbol="NEGVOL", volume=-1),
        _row(symbol="HILO", high=10.0, low=20.0),
        _row(symbol="CLOSEOOB", close=9999.0),
        _row(symbol="NOKEY", instrument_key=None),
        _row(symbol="EMPTYKEY", instrument_key=""),
    ])[config.CANON_COLUMNS]
    clean, bad = quarantine_bad_rows(df)
    assert set(clean["symbol"]) == {"GOOD"}
    assert set(bad["symbol"]) == {"NEGVOL", "HILO", "CLOSEOOB", "NOKEY", "EMPTYKEY"}


def test_quarantine_handles_empty_frame():
    empty = pd.DataFrame([_row()])[config.CANON_COLUMNS].iloc[0:0]
    clean, bad = quarantine_bad_rows(empty)
    assert len(clean) == 0
    assert len(bad) == 0


# --- check_rowcount_by_series (Task 4: per-series gate) ---------------------


def test_by_series_total_outside_abs_range_fails():
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            1000, {"EQ": 1000}, {"EQ": [1000, 1000]}, abs_range=(2000, 10000)
        )


def test_by_series_total_within_abs_range_and_stable_passes():
    check_rowcount_by_series(
        2500, {"EQ": 2500}, {"EQ": [2500, 2500, 2500]}, abs_range=(2000, 10000)
    )  # no raise


def test_by_series_uses_config_abs_range_by_default(monkeypatch):
    monkeypatch.setattr(config, "ROWCOUNT_ABS_RANGE", (100, 200))
    check_rowcount_by_series(150, {"EQ": 150}, {"EQ": [150]})  # no raise
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(50, {"EQ": 50}, {"EQ": [150]})


def test_by_series_deviation_over_threshold_fails():
    # EQ deviates ~50% from its trailing mean of 2000 -> must fail even though
    # the total is within abs_range.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            3000, {"EQ": 1000, "BE": 2000}, {"EQ": [2000, 2000, 2000], "BE": [2000, 2000]},
            abs_range=(2000, 10000),
        )


def test_by_series_new_series_with_no_trailing_passes():
    # A series with an empty trailing list is "new today" -- it accumulates
    # history instead of failing.
    check_rowcount_by_series(
        2100, {"EQ": 2000, "BE": 100}, {"EQ": [2000, 2000], "BE": []}, abs_range=(2000, 10000)
    )  # no raise


def test_by_series_major_series_vanished_fails():
    # BE had a trailing mean >= 50 but is absent from today's series_counts ->
    # a truncation signal, must fail even though EQ alone is fine.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            2000, {"EQ": 2000}, {"EQ": [2000, 2000], "BE": [60, 55, 58]},
            abs_range=(2000, 10000),
        )


def test_by_series_minor_series_vanished_passes():
    # A series with trailing mean < 50 disappearing is not treated as a
    # truncation signal.
    check_rowcount_by_series(
        2000, {"EQ": 2000}, {"EQ": [2000, 2000], "BZ": [3, 4, 2]}, abs_range=(2000, 10000)
    )  # no raise


def test_migration_widened_day_against_eq_only_trailing_passes():
    # The core migration property: a widened day (~2x rows via new series,
    # EQ itself roughly stable) validated against EQ-ONLY trailing history
    # (pre-migration data) must PASS.
    trailing = {"EQ": [2000, 2010, 1995]}  # EQ-only history, no BE/BZ trailing yet
    today_series = {"EQ": 2005, "BE": 1800, "BZ": 200}  # ~4000 total, new series
    total = sum(today_series.values())
    check_rowcount_by_series(total, today_series, trailing, abs_range=(2000, 10000))  # no raise


def test_migration_truncated_eq_file_fails():
    # A truncated file: EQ count halved relative to EQ-only trailing history.
    # Even with new series present, the EQ deviation must trip the gate.
    trailing = {"EQ": [2000, 2010, 1995]}
    today_series = {"EQ": 1000, "BE": 1800, "BZ": 200}  # EQ halved
    total = sum(today_series.values())
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(total, today_series, trailing, abs_range=(2000, 10000))


# --- G3 backfill live finding: sub-floor deviation exemption ----------------
# Real 10-day multi-day backfill against live NSE bhavcopy tripped the
# per-series DEVIATION check on tiny series (the empty-series "" bucket from
# null-SctySrs rows, thin bond/misc series like BZ/GS) where a normal
# 4->6-row wobble is +50%, trivially exceeding the 15% band. The ABSENCE
# check already exempts mean < 50; the DEVIATION check must match it.


def test_tiny_series_deviation_is_exempt():
    # series "" trailing [4, 4, 5] (mean ~4.3), today 6 -- a +50%-ish wobble
    # in a sub-50-mean bucket must NOT raise. EQ is included alongside it
    # (stable, large) so the call reflects a realistic multi-series day.
    check_rowcount_by_series(
        2384 + 6,
        {"EQ": 2384, "": 6},
        {"EQ": [2384, 2380, 2390], "": [4, 4, 5]},
        abs_range=(2000, 10000),
    )  # no raise


def test_large_series_deviation_still_fails():
    # EQ trailing mean ~2385, today 1200 -- a real truncation. The sub-50
    # exemption must not weaken protection for series at/above the floor.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            1200,
            {"EQ": 1200},
            {"EQ": [2384, 2380, 2390]},
            abs_range=(1000, 10000),
        )


def test_absence_floor_uses_named_constant():
    # mean >= 50 absent today -> still fails (unchanged truncation signal).
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            2000, {"EQ": 2000}, {"EQ": [2000, 2000], "BE": [60, 55, 58]},
            abs_range=(2000, 10000),
        )
    # mean < 50 absent today -> still exempt (unchanged).
    check_rowcount_by_series(
        2000, {"EQ": 2000}, {"EQ": [2000, 2000], "BZ": [3, 4, 2]}, abs_range=(2000, 10000)
    )  # no raise
    assert config.SERIES_MIN_FOR_GATE == 50


def test_series_mean_exactly_at_floor_is_gated():
    # Boundary: mean == SERIES_MIN_FOR_GATE (50) is >= floor, so the
    # deviation gate still APPLIES (only mean < floor is exempt) -- it is
    # NOT treated as tiny/exempt. Since 50 < SERIES_LARGE_MEAN (1000), the
    # LOOSE 50% band applies at this boundary (superseded by the G3
    # 300-day backfill size-tiering fix below: a +20% swing here, e.g. GS's
    # real observed churn, is business-as-usual and must NOT raise; only a
    # swing beyond the loose band still fails).
    check_rowcount_by_series(
        2384 + 60,
        {"EQ": 2384, "GS": 60},
        {"EQ": [2384, 2380, 2390], "GS": [50, 50, 50]},
        abs_range=(2000, 10000),
    )  # no raise -- +20% is within the loose 50% band for a mean-50 series
    # But the gate still applies (not exempt): a swing beyond the loose
    # band at this same boundary mean still fails.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            2384 + 200,
            {"EQ": 2384, "GS": 200},
            {"EQ": [2384, 2380, 2390], "GS": [50, 50, 50]},
            abs_range=(2000, 10000),
        )


# --- G3 300-day backfill live finding: size-tiered deviation tolerance -----
# The real 300-day backfill (68/300 days, 23%) failed the per-series
# deviation gate on MID-SIZE series -- a deeper layer than the earlier
# sub-50 fix above. Real observed values are natural policy-driven
# membership churn in NSE's surveillance/trade-to-trade/govt segments, NOT
# truncations:
#   BE ~266 -> 164..187 (up to -38%)
#   ST ~67-143 -> 78..120 (+-17-30%)
#   GS ~51 -> 36 (-29%)
#   GB ~51 -> 42 (-18%)
# Stable anchors, no failures at the tight 15% band: EQ ~2384, SM ~302.
# A flat 15% band is statistically wrong for churny mid-size segments (15%
# of EQ's 2384 rows is ~7 sigma; 15% of a 51-row surveillance segment is
# business-as-usual). Fix: size-tiered tolerance, decoupled from the
# absence floor -- mean >= SERIES_LARGE_MEAN (1000) keeps the tight 15%
# band (only EQ qualifies); 50 <= mean < 1000 gets a loose 50% band
# (tightened from 60% per reviewer follow-up, see config.py).


def test_midsize_series_natural_churn_is_tolerated():
    # Real observed G3 backfill values, all 50 <= mean < 1000 (loose 50%
    # band) -- natural surveillance/trade-to-trade/govt segment churn, none
    # of this is a truncation and none should raise.
    check_rowcount_by_series(
        2384 + 164 + 36 + 119 + 42,
        {"EQ": 2384, "BE": 164, "GS": 36, "ST": 119, "GB": 42},
        {
            "EQ": [2384, 2380, 2390],  # large anchor, stable
            "BE": [266, 266, 266],  # -38.3%, natural churn
            "GS": [51, 51, 51],  # -29.4%, natural churn
            "ST": [143, 143, 143],  # -16.8%, natural churn
            "GB": [51, 51, 51],  # -17.6%, natural churn
        },
        abs_range=(2000, 10000),
    )  # no raise


def test_large_anchor_series_keeps_tight_band():
    # EQ trailing mean ~2385 (>= SERIES_LARGE_MEAN) keeps the tight 15%
    # band: a real truncation must still be caught.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            1200,
            {"EQ": 1200},  # -50%, truncation
            {"EQ": [2384, 2380, 2390]},
            abs_range=(1000, 10000),
        )
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            2000,
            {"EQ": 2000},  # ~-16%, exceeds the tight 15% band
            {"EQ": [2384, 2380, 2390]},
            abs_range=(2000, 10000),
        )


def test_midsize_egregious_collapse_still_fails():
    # BE mean 266, today 100 (-62.4%) exceeds even the loose 50% band -- a
    # real collapse in a mid-size series must still be caught.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            100,
            {"BE": 100},
            {"BE": [266, 266, 266]},
            abs_range=(0, 10000),
        )


def test_midsize_tightened_band_now_catches_55_percent_drop():
    # The whole point of tightening 60% -> 50% (reviewer follow-up): a
    # ~55% drop in a mid-size series -- BE mean 266, today 120 (-54.9%) --
    # used to PASS under the old 60% band, silently storing a real
    # isolated truncation. Under the tightened 50% band it must now FAIL.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            120,
            {"BE": 120},
            {"BE": [266, 266, 266]},
            abs_range=(0, 10000),
        )


def test_midsize_loose_band_still_guards_a_stable_series():
    # SM is a stable, larger mid-tier series (mean ~302, still < the 1000
    # SERIES_LARGE_MEAN cutoff, so it gets the loose 50% band). A real
    # >50% collapse -- SM mean 302, today 140 (-53.6%) -- must still be
    # caught: the loose band tolerates natural churn, not a genuine
    # truncation, even for a series well above the SERIES_MIN_FOR_GATE
    # floor.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            140,
            {"SM": 140},
            {"SM": [302, 302, 302]},
            abs_range=(0, 10000),
        )


def test_tiny_series_still_exempt():
    # Unchanged sub-50 exemption: series "" mean ~4, today 6 -- no raise.
    check_rowcount_by_series(
        6,
        {"": 6},
        {"": [4, 4, 4]},
        abs_range=(0, 10000),
    )  # no raise


def test_midsize_vanished_entirely_still_fails():
    # BE mean ~266 (>= SERIES_MIN_FOR_GATE) absent from today's
    # series_counts -- the absence check is decoupled from the loose
    # deviation band and must still fail.
    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            2384,
            {"EQ": 2384},
            {"EQ": [2384, 2380, 2390], "BE": [266, 266, 266]},
            abs_range=(2000, 10000),
        )


def test_series_size_tier_boundary_at_1000():
    # Boundary: mean == SERIES_LARGE_MEAN (1000) -> tight 15% band applies
    # (>= is tight). mean == 999 -> loose 50% band applies (< is loose).
    # A 999-mean series at +30% passes (loose); a 1000-mean series at +30%
    # fails (tight).
    check_rowcount_by_series(
        1299,
        {"X": 1299},  # 999 * 1.30 == 1298.7 -> +30%
        {"X": [999, 999, 999]},
        abs_range=(0, 10000),
    )  # no raise -- loose band tolerates +30%

    with pytest.raises(UnexpectedFailure):
        check_rowcount_by_series(
            1300,
            {"X": 1300},  # 1000 * 1.30 == 1300 -> +30%
            {"X": [1000, 1000, 1000]},
            abs_range=(0, 10000),
        )  # tight band trips at +30%
