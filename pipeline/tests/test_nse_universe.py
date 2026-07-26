from __future__ import annotations

from datetime import date

from pipeline.sources.nse_universe import (
    ActiveNseEquity,
    RegistryEntry,
    RegistryStatus,
    parse_active_nse_equities,
    run_incremental_collection,
)
from pipeline.sources.screener_classification import ClassificationObservation


def _observation() -> ClassificationObservation:
    return ClassificationObservation(
        macro_sector="Energy",
        sector="Oil, Gas & Consumable Fuels",
        industry="Petroleum Products",
        basic_industry="Refineries & Marketing",
    )


def test_official_equity_snapshot_keeps_only_active_equity_series():
    snapshot = parse_active_nse_equities(
        b"SYMBOL, SERIES, ISIN NUMBER\nRELIANCE, EQ, INE002A01018\nBOND, N1, INE000A00001\n"
    )

    assert snapshot == (ActiveNseEquity("INE002A01018", "RELIANCE"),)


def test_new_isin_is_collected_without_reindexing_known_records():
    records = {
        "INE002A01018": RegistryEntry.classified("INE002A01018", "RELIANCE", _observation()),
        "INE009A01021": RegistryEntry.classified("INE009A01021", "INFY", _observation()),
    }
    calls: list[str] = []

    result = run_incremental_collection(
        records,
        [
            ActiveNseEquity("INE002A01018", "RELIANCE"),
            ActiveNseEquity("INE999A01019", "NEWCO"),
        ],
        today=date(2026, 7, 24),
        collect=lambda symbol: calls.append(symbol) or _observation(),
    )

    assert calls == ["NEWCO"]
    assert result.records["INE002A01018"] == records["INE002A01018"]
    assert result.records["INE999A01019"].status is RegistryStatus.CLASSIFIED


def test_invalid_snapshot_leaves_registry_and_collector_untouched():
    records = {
        "INE002A01018": RegistryEntry.classified("INE002A01018", "RELIANCE", _observation()),
        "INE009A01021": RegistryEntry.classified("INE009A01021", "INFY", _observation()),
    }
    calls: list[str] = []

    result = run_incremental_collection(
        records,
        [
            ActiveNseEquity("INE002A01018", "RELIANCE"),
            ActiveNseEquity("INE002A01018", "DUPLICATE"),
        ],
        today=date(2026, 7, 24),
        collect=lambda symbol: calls.append(symbol) or _observation(),
    )

    assert result.valid_snapshot is False
    assert result.records == records
    assert calls == []


def test_two_valid_absences_inactivate_and_return_reactivates_an_isin():
    records = {
        "INE002A01018": RegistryEntry.classified("INE002A01018", "RELIANCE", _observation()),
        "INE009A01021": RegistryEntry.classified("INE009A01021", "INFY", _observation()),
    }

    still_active = [ActiveNseEquity("INE009A01021", "INFY")]
    first = run_incremental_collection(
        records, still_active, today=date(2026, 7, 24), collect=lambda _: None
    )
    second = run_incremental_collection(
        first.records, still_active, today=date(2026, 7, 25), collect=lambda _: None
    )
    returned = run_incremental_collection(
        second.records,
        [
            ActiveNseEquity("INE002A01018", "RELIANCE"),
            ActiveNseEquity("INE009A01021", "INFY"),
        ],
        today=date(2026, 7, 28),
        collect=lambda _: None,
    )

    assert first.records["INE002A01018"].status is RegistryStatus.CLASSIFIED
    assert second.records["INE002A01018"].status is RegistryStatus.INACTIVE
    assert returned.records["INE002A01018"].status is RegistryStatus.CLASSIFIED


def test_retry_budget_defers_a_failed_record_after_the_third_retry():
    records = {"INE222A01022": RegistryEntry.pending("INE222A01022", "RETRYCO")}
    for today in (date(2026, 7, 24), date(2026, 7, 25), date(2026, 7, 28), date(2026, 8, 4)):
        result = run_incremental_collection(
            records,
            [ActiveNseEquity("INE222A01022", "RETRYCO")],
            today=today,
            collect=lambda _: None,
        )
        records = result.records

    entry = records["INE222A01022"]
    assert entry.deferred_to_full_audit is True
    assert entry.retry_on is None


def test_renamed_and_due_retry_records_are_the_only_existing_collection_candidates():
    records = {
        "INE002A01018": RegistryEntry.classified("INE002A01018", "RELIANCE", _observation()),
        "INE111A01011": RegistryEntry.classified("INE111A01011", "OLDNAME", _observation()),
        "INE222A01022": RegistryEntry.pending(
            "INE222A01022", "RETRYCO", retry_on=date(2026, 7, 24)
        ),
    }
    calls: list[str] = []

    result = run_incremental_collection(
        records,
        [
            ActiveNseEquity("INE002A01018", "RELIANCE"),
            ActiveNseEquity("INE111A01011", "NEWNAME"),
            ActiveNseEquity("INE222A01022", "RETRYCO"),
        ],
        today=date(2026, 7, 24),
        collect=lambda symbol: calls.append(symbol) or _observation(),
    )

    assert calls == ["NEWNAME", "RETRYCO"]
    assert result.records["INE111A01011"].symbol == "NEWNAME"
    assert result.records["INE222A01022"].status is RegistryStatus.CLASSIFIED
