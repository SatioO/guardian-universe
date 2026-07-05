"""Weekly source cross-check: sampled close-price comparison across the two
INDEPENDENT NSE endpoints (UDiFF primary, sec_bhavdata_full secondary). Pure
logic -- both sources are already fetched, normalized to CANONICAL, and
handed in as frames; no I/O here.

Why this exists: a corrupted primary source is currently undetectable when
BOTH sources are individually "healthy" (each passes its own gates, but one
silently serves wrong numbers) -- there is no signal today that would catch
that. Comparing a deterministic sample of closes between the two
INDEPENDENT endpoints (constructed WITHOUT fallbacks, so each is tested in
isolation -- see cli.py's cross-check command) surfaces a silent divergence
that neither source's own validation would ever flag on its own.

Coverage limitation (read before trusting a clean result): both input frames
are CANONICAL, joined on `instrument_key`. The secondary (secfull) side has
no ISIN column of its own -- its instrument_key is resolved via the same
isin_map used elsewhere (see datasets._load_isin_map), and a symbol missing
from that map gets an "NSE:"+symbol SENTINEL key instead of a real ISIN.
Sentinel keys essentially never equal the primary side's real-ISIN keys, so
those rows silently drop out of the inner join -- they are not "compared and
found to agree", they are simply never compared at all. The comparison only
covers the MAPPED INTERSECTION of the two sources; that is the meaningful
set for this check, but it is not the full universe."""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class CrossCheckResult:
    compared: int
    mismatched: int
    # Up to 5 worst mismatches by relative divergence, descending:
    # (instrument_key, primary_close, secondary_close).
    worst: list[tuple[str, float, float]]


_WORST_LIMIT = 5


def compare_sources(
    primary_df: pd.DataFrame,
    secondary_df: pd.DataFrame,
    *,
    sample_n: int = 50,
    tolerance: float = 0.001,
    seed_symbols: list[str] | None = None,
) -> CrossCheckResult:
    """Compare a deterministic sample of `close` values between two CANONICAL
    frames, joined on `instrument_key`.

    Sampling is deterministic -- NO randomness (no Date.now/random anywhere
    in this path): the joined keys are sorted, then every k-th key is taken,
    where k = max(1, len(joined) // sample_n). The same two input frames
    always produce the identical sampled set and therefore an identical
    result.

    `seed_symbols`, when given, are always included in the compared sample
    (in addition to the deterministic stride) -- e.g. to guarantee a
    known-liquid bellwether symbol is never skipped by the stride regardless
    of population size. Missing entirely from `seed_symbols` (None/empty)
    changes nothing about the base deterministic-stride behavior.

    A pair is a mismatch when the relative divergence
    (|primary_close - secondary_close| / |primary_close|) exceeds
    `tolerance` -- exactly-at-tolerance is NOT a mismatch (a "<=" gate).

    Coverage note: this only ever sees rows present in BOTH frames after the
    inner join -- see this module's docstring for why the secondary side's
    isin_map-sentinel keys mean less-than-full-universe coverage is expected
    and not itself a bug.
    """
    joined = primary_df[["instrument_key", "close"]].merge(
        secondary_df[["instrument_key", "close"]],
        on="instrument_key",
        how="inner",
        suffixes=("_primary", "_secondary"),
    )
    if joined.empty:
        return CrossCheckResult(compared=0, mismatched=0, worst=[])

    joined = joined.sort_values("instrument_key").reset_index(drop=True)

    stride = max(1, len(joined) // sample_n)
    sampled = joined.iloc[::stride]

    if seed_symbols:
        seeds = joined[joined["instrument_key"].isin(seed_symbols)]
        sampled = pd.concat([sampled, seeds]).drop_duplicates(
            subset="instrument_key"
        ).sort_values("instrument_key")

    compared = len(sampled)
    divergence = (
        (sampled["close_primary"] - sampled["close_secondary"]).abs()
        / sampled["close_primary"].abs()
    )
    mismatch_mask = divergence > tolerance
    mismatched = int(mismatch_mask.sum())

    worst_df = (
        sampled.assign(_divergence=divergence)[mismatch_mask.to_numpy()]
        .sort_values("_divergence", ascending=False)
        .head(_WORST_LIMIT)
    )
    worst = [
        (str(key), float(primary_close), float(secondary_close))
        for key, primary_close, secondary_close in zip(
            worst_df["instrument_key"], worst_df["close_primary"], worst_df["close_secondary"],
            strict=True,
        )
    ]

    return CrossCheckResult(compared=compared, mismatched=mismatched, worst=worst)
