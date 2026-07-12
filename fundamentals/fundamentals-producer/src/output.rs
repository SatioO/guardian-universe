//! `fundamentals.parquet` writer/reader.
//!
//! Determinism is load-bearing (the Phase-1 idempotency milestone): stable
//! sort, fixed writer properties (zstd, fixed `created_by`, single sorted row
//! group), atomic tmp+rename. Writing the same rows twice must produce
//! byte-identical files.

use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::rows::FundRow;

pub fn schema() -> Schema {
    let utf8 = |n: &str| Field::new(n, DataType::Utf8, false);
    let f64_null = |n: &str| Field::new(n, DataType::Float64, true);
    Schema::new(vec![
        // `date` mirrors `as_of` (ISO): the pipeline's manifest machinery
        // reads latest_date/row counts off a `date` column for every dataset
        // (the same reason sector_industry appends one). Not part of FundRow —
        // written from `as_of`, ignored on read.
        utf8("date"),
        utf8("instrument_key"),
        utf8("symbol"),
        utf8("period_end"),
        utf8("fiscal_quarter"),
        utf8("basis"),
        Field::new("is_restated", DataType::Boolean, false),
        utf8("sector_kind"),
        f64_null("revenue"),
        f64_null("operating_profit"),
        f64_null("opm_pct"),
        utf8("margin_kind"),
        f64_null("other_income"),
        f64_null("interest"),
        f64_null("depreciation"),
        f64_null("pbt"),
        f64_null("tax"),
        f64_null("net_profit"),
        f64_null("eps"),
        // ── Phase-3 sector union (nullable; NULL for non-applicable sectors,
        // NULL on every general row). Appending nullable columns is the
        // non-breaking parquet evolution path: the client reads by name and
        // fails closed (missing value → excluded), never crashes.
        f64_null("total_income"),
        f64_null("interest_earned"),
        f64_null("net_interest_income"),
        f64_null("nim_pct"),
        f64_null("interest_expended"),
        f64_null("operating_expenses"),
        f64_null("pre_provision_operating_profit"),
        f64_null("provisions_and_contingencies"),
        f64_null("gross_npa_pct"),
        f64_null("net_npa_pct"),
        f64_null("impairment_on_financial_instruments"),
        f64_null("gross_stage3_pct"),
        f64_null("gross_premium_income"),
        f64_null("net_premium_income"),
        f64_null("investment_income"),
        f64_null("net_commission"),
        f64_null("benefits_paid"),
        f64_null("combined_ratio_pct"),
        f64_null("solvency_ratio"),
        f64_null("equity"),
        f64_null("total_debt"),
        f64_null("cash"),
        f64_null("shares_outstanding"),
        f64_null("face_value"),
        f64_null("ebitda_annual"),
        f64_null("capital_employed"),
        f64_null("ttm_eps"),
        f64_null("book_value_per_share"),
        utf8("as_of"),
        utf8("source_channel"),
        Field::new("fields_resolved_pct", DataType::Float64, false),
        utf8("dq_flags"),
        Field::new("is_audited", DataType::Boolean, false),
    ])
}

/// Stable output order: instrument_key asc, period_end DESC (newest first),
/// fiscal_quarter asc ("FY" < "Q1" < … lexically is fine — just stable),
/// basis asc.
pub fn sort_rows(rows: &mut [FundRow]) {
    rows.sort_by(|a, b| {
        a.instrument_key
            .cmp(&b.instrument_key)
            .then_with(|| b.period_end.cmp(&a.period_end))
            .then_with(|| a.fiscal_quarter.cmp(&b.fiscal_quarter))
            .then_with(|| a.basis.cmp(&b.basis))
    });
}

fn to_batch(rows: &[FundRow]) -> Result<RecordBatch, String> {
    let mut s_date = StringBuilder::new();
    let mut s_instrument = StringBuilder::new();
    let mut s_symbol = StringBuilder::new();
    let mut s_period_end = StringBuilder::new();
    let mut s_fq = StringBuilder::new();
    let mut s_basis = StringBuilder::new();
    let mut b_restated = BooleanBuilder::new();
    let mut s_sector = StringBuilder::new();
    let mut f: Vec<Float64Builder> = (0..38).map(|_| Float64Builder::new()).collect();
    let mut s_margin = StringBuilder::new();
    let mut s_as_of = StringBuilder::new();
    let mut s_channel = StringBuilder::new();
    let mut f_resolved = Float64Builder::new();
    let mut s_flags = StringBuilder::new();
    let mut b_audited = BooleanBuilder::new();

    for r in rows {
        s_date.append_value(&r.as_of);
        s_instrument.append_value(&r.instrument_key);
        s_symbol.append_value(&r.symbol);
        s_period_end.append_value(&r.period_end);
        s_fq.append_value(&r.fiscal_quarter);
        s_basis.append_value(&r.basis);
        b_restated.append_value(r.is_restated);
        s_sector.append_value(&r.sector_kind);
        let numeric = [
            r.revenue,
            r.operating_profit,
            r.opm_pct,
            r.other_income,
            r.interest,
            r.depreciation,
            r.pbt,
            r.tax,
            r.net_profit,
            r.eps,
            r.total_income,
            r.interest_earned,
            r.net_interest_income,
            r.nim_pct,
            r.interest_expended,
            r.operating_expenses,
            r.pre_provision_operating_profit,
            r.provisions_and_contingencies,
            r.gross_npa_pct,
            r.net_npa_pct,
            r.impairment_on_financial_instruments,
            r.gross_stage3_pct,
            r.gross_premium_income,
            r.net_premium_income,
            r.investment_income,
            r.net_commission,
            r.benefits_paid,
            r.combined_ratio_pct,
            r.solvency_ratio,
            r.equity,
            r.total_debt,
            r.cash,
            r.shares_outstanding,
            r.face_value,
            r.ebitda_annual,
            r.capital_employed,
            r.ttm_eps,
            r.book_value_per_share,
        ];
        for (i, v) in numeric.iter().enumerate() {
            f[i].append_option(*v);
        }
        s_margin.append_value(&r.margin_kind);
        s_as_of.append_value(&r.as_of);
        s_channel.append_value(&r.source_channel);
        f_resolved.append_value(r.fields_resolved_pct);
        s_flags.append_value(&r.dq_flags);
        b_audited.append_value(r.is_audited);
    }

    let mut fi = f.into_iter();
    let mut next_f = || -> ArrayRef { Arc::new(fi.next().expect("builder count").finish()) };

    let columns: Vec<ArrayRef> = vec![
        Arc::new(s_date.finish()),
        Arc::new(s_instrument.finish()),
        Arc::new(s_symbol.finish()),
        Arc::new(s_period_end.finish()),
        Arc::new(s_fq.finish()),
        Arc::new(s_basis.finish()),
        Arc::new(b_restated.finish()),
        Arc::new(s_sector.finish()),
        next_f(), // revenue
        next_f(), // operating_profit
        next_f(), // opm_pct
        Arc::new(s_margin.finish()),
        next_f(), // other_income
        next_f(), // interest
        next_f(), // depreciation
        next_f(), // pbt
        next_f(), // tax
        next_f(), // net_profit
        next_f(), // eps
        next_f(), // total_income
        next_f(), // interest_earned
        next_f(), // net_interest_income
        next_f(), // nim_pct
        next_f(), // interest_expended
        next_f(), // operating_expenses
        next_f(), // pre_provision_operating_profit
        next_f(), // provisions_and_contingencies
        next_f(), // gross_npa_pct
        next_f(), // net_npa_pct
        next_f(), // impairment_on_financial_instruments
        next_f(), // gross_stage3_pct
        next_f(), // gross_premium_income
        next_f(), // net_premium_income
        next_f(), // investment_income
        next_f(), // net_commission
        next_f(), // benefits_paid
        next_f(), // combined_ratio_pct
        next_f(), // solvency_ratio
        next_f(), // equity
        next_f(), // total_debt
        next_f(), // cash
        next_f(), // shares_outstanding
        next_f(), // face_value
        next_f(), // ebitda_annual
        next_f(), // capital_employed
        next_f(), // ttm_eps
        next_f(), // book_value_per_share
        Arc::new(s_as_of.finish()),
        Arc::new(s_channel.finish()),
        Arc::new(f_resolved.finish()),
        Arc::new(s_flags.finish()),
        Arc::new(b_audited.finish()),
    ];

    RecordBatch::try_new(Arc::new(schema()), columns).map_err(|e| format!("record batch: {e}"))
}

/// Serialize rows to parquet bytes (deterministic for identical input).
pub fn to_parquet_bytes(rows: &[FundRow]) -> Result<Vec<u8>, String> {
    let batch = to_batch(rows)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(9).expect("valid zstd level")))
        .set_created_by("fundamentals-producer".to_string())
        .set_max_row_group_size(1_048_576)
        .build();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, Arc::new(schema()), Some(props))
        .map_err(|e| format!("parquet writer: {e}"))?;
    writer.write(&batch).map_err(|e| format!("parquet write: {e}"))?;
    writer.close().map_err(|e| format!("parquet close: {e}"))?;
    Ok(buf)
}

/// Atomic write: tmp file in the same directory + rename.
pub fn write_parquet(path: &Path, rows: &[FundRow]) -> Result<(), String> {
    let bytes = to_parquet_bytes(rows)?;
    let tmp = path.with_extension("parquet.tmp");
    std::fs::write(&tmp, &bytes).map_err(|e| format!("write {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("rename {}: {e}", path.display()))
}

/// Read all rows back (the parquet file doubles as the accumulated store).
pub fn read_parquet(path: &Path) -> Result<Vec<FundRow>, String> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("parquet reader: {e}"))?
        .build()
        .map_err(|e| format!("parquet reader build: {e}"))?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| format!("parquet batch: {e}"))?;
        rows.extend(batch_to_rows(&batch)?);
    }
    Ok(rows)
}

fn batch_to_rows(batch: &RecordBatch) -> Result<Vec<FundRow>, String> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;

    let s = |name: &str| -> Result<&arrow_array::StringArray, String> {
        Ok(batch
            .column_by_name(name)
            .ok_or_else(|| format!("missing column {name}"))?
            .as_string::<i32>())
    };
    let fcol = |name: &str| -> Result<&arrow_array::PrimitiveArray<Float64Type>, String> {
        Ok(batch
            .column_by_name(name)
            .ok_or_else(|| format!("missing column {name}"))?
            .as_primitive::<Float64Type>())
    };
    // Nullable column that may be ABSENT in accumulated parquets written by
    // an older producer (schema evolution: the Phase-2 file predates the
    // sector-union columns). Absent column → every row reads None; the next
    // write emits the full current schema.
    let fcol_opt = |name: &str| -> Option<&arrow_array::PrimitiveArray<Float64Type>> {
        batch
            .column_by_name(name)
            .map(|c| c.as_primitive::<Float64Type>())
    };
    let b = |name: &str| -> Result<&arrow_array::BooleanArray, String> {
        Ok(batch
            .column_by_name(name)
            .ok_or_else(|| format!("missing column {name}"))?
            .as_boolean())
    };

    let opt = |arr: &arrow_array::PrimitiveArray<Float64Type>, i: usize| -> Option<f64> {
        use arrow_array::Array;
        if arr.is_null(i) { None } else { Some(arr.value(i)) }
    };

    let (instrument_key, symbol, period_end, fiscal_quarter, basis, sector_kind) = (
        s("instrument_key")?, s("symbol")?, s("period_end")?, s("fiscal_quarter")?, s("basis")?, s("sector_kind")?,
    );
    let (margin_kind, as_of, source_channel, dq_flags) =
        (s("margin_kind")?, s("as_of")?, s("source_channel")?, s("dq_flags")?);
    let (is_restated, is_audited) = (b("is_restated")?, b("is_audited")?);
    let fields_resolved_pct = fcol("fields_resolved_pct")?;

    let numeric_names = [
        "revenue", "operating_profit", "opm_pct", "other_income", "interest",
        "depreciation", "pbt", "tax", "net_profit", "eps", "equity", "total_debt",
        "cash", "shares_outstanding", "face_value", "ebitda_annual",
        "capital_employed", "ttm_eps", "book_value_per_share",
    ];
    let mut numeric = Vec::with_capacity(numeric_names.len());
    for n in numeric_names {
        numeric.push(fcol(n)?);
    }
    // Phase-3 sector union: read leniently (may be absent in older files).
    let sector_names = [
        "total_income", "interest_earned", "net_interest_income", "nim_pct",
        "interest_expended", "operating_expenses", "pre_provision_operating_profit",
        "provisions_and_contingencies", "gross_npa_pct", "net_npa_pct",
        "impairment_on_financial_instruments", "gross_stage3_pct",
        "gross_premium_income", "net_premium_income", "investment_income",
        "net_commission", "benefits_paid", "combined_ratio_pct", "solvency_ratio",
    ];
    let sector_cols: Vec<Option<&arrow_array::PrimitiveArray<Float64Type>>> =
        sector_names.iter().map(|n| fcol_opt(n)).collect();

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let n = |j: usize| opt(numeric[j], i);
        let sn = |j: usize| sector_cols[j].and_then(|arr| opt(arr, i));
        rows.push(FundRow {
            instrument_key: instrument_key.value(i).to_string(),
            symbol: symbol.value(i).to_string(),
            period_end: period_end.value(i).to_string(),
            fiscal_quarter: fiscal_quarter.value(i).to_string(),
            basis: basis.value(i).to_string(),
            is_restated: is_restated.value(i),
            sector_kind: sector_kind.value(i).to_string(),
            revenue: n(0),
            operating_profit: n(1),
            opm_pct: n(2),
            margin_kind: margin_kind.value(i).to_string(),
            other_income: n(3),
            interest: n(4),
            depreciation: n(5),
            pbt: n(6),
            tax: n(7),
            net_profit: n(8),
            eps: n(9),
            total_income: sn(0),
            interest_earned: sn(1),
            net_interest_income: sn(2),
            nim_pct: sn(3),
            interest_expended: sn(4),
            operating_expenses: sn(5),
            pre_provision_operating_profit: sn(6),
            provisions_and_contingencies: sn(7),
            gross_npa_pct: sn(8),
            net_npa_pct: sn(9),
            impairment_on_financial_instruments: sn(10),
            gross_stage3_pct: sn(11),
            gross_premium_income: sn(12),
            net_premium_income: sn(13),
            investment_income: sn(14),
            net_commission: sn(15),
            benefits_paid: sn(16),
            combined_ratio_pct: sn(17),
            solvency_ratio: sn(18),
            equity: n(10),
            total_debt: n(11),
            cash: n(12),
            shares_outstanding: n(13),
            face_value: n(14),
            ebitda_annual: n(15),
            capital_employed: n(16),
            ttm_eps: n(17),
            book_value_per_share: n(18),
            as_of: as_of.value(i).to_string(),
            source_channel: source_channel.value(i).to_string(),
            fields_resolved_pct: fields_resolved_pct.value(i),
            dq_flags: dq_flags.value(i).to_string(),
            is_audited: is_audited.value(i),
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_rows() -> Vec<FundRow> {
        let mut r1 = FundRow {
            instrument_key: "INE690A01028".into(),
            symbol: "TTKPRESTIG".into(),
            period_end: "2026-03-31".into(),
            fiscal_quarter: "Q4".into(),
            basis: "standalone".into(),
            is_restated: false,
            sector_kind: "general".into(),
            revenue: Some(679.57),
            operating_profit: Some(56.4),
            opm_pct: Some(8.3),
            margin_kind: "opm".into(),
            other_income: Some(17.62),
            interest: Some(2.22),
            depreciation: Some(21.71),
            pbt: Some(69.7),
            tax: Some(18.91),
            net_profit: Some(50.79),
            eps: Some(3.71),
            equity: Some(1993.28),
            total_debt: None,
            cash: Some(31.32),
            shares_outstanding: Some(1.37e8),
            face_value: Some(1.0),
            ebitda_annual: None,
            capital_employed: Some(1993.28),
            ttm_eps: Some(13.54),
            book_value_per_share: Some(145.49),
            as_of: "2026-07-12".into(),
            source_channel: "bse".into(),
            fields_resolved_pct: 1.0,
            dq_flags: String::new(),
            is_audited: true,
            ..Default::default()
        };
        let mut r2 = r1.clone();
        r2.fiscal_quarter = "FY".into();
        r2.revenue = Some(2772.69);
        r2.eps = Some(13.54);
        r2.ebitda_annual = Some(334.55);
        let mut r3 = r1.clone();
        r3.instrument_key = "INE117A01022".into();
        r3.symbol = "ABB".into();
        // Deliberately unsorted input.
        std::mem::swap(&mut r1, &mut r3);
        vec![r1, r2, r3]
    }

    #[test]
    fn parquet_round_trips_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fundamentals.parquet");
        let mut rows = sample_rows();
        sort_rows(&mut rows);
        write_parquet(&path, &rows).unwrap();
        let back = read_parquet(&path).unwrap();
        assert_eq!(rows, back);
    }

    #[test]
    fn identical_rows_serialize_byte_identically() {
        let mut rows = sample_rows();
        sort_rows(&mut rows);
        let a = to_parquet_bytes(&rows).unwrap();
        let b = to_parquet_bytes(&rows).unwrap();
        assert_eq!(a, b, "parquet serialization must be deterministic");
    }

    #[test]
    fn write_read_write_is_byte_identical() {
        // The full idempotency path: write, read back, write again.
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a.parquet");
        let p2 = dir.path().join("b.parquet");
        let mut rows = sample_rows();
        sort_rows(&mut rows);
        write_parquet(&p1, &rows).unwrap();
        let mut back = read_parquet(&p1).unwrap();
        sort_rows(&mut back);
        write_parquet(&p2, &back).unwrap();
        assert_eq!(std::fs::read(&p1).unwrap(), std::fs::read(&p2).unwrap());
    }

    #[test]
    fn sort_is_stable_and_newest_first_per_symbol() {
        let mut rows = sample_rows();
        sort_rows(&mut rows);
        assert_eq!(rows[0].instrument_key, "INE117A01022");
        assert_eq!(rows[1].instrument_key, "INE690A01028");
        // FY sorts before Q4 at the same period_end (lexical, stable).
        assert_eq!(rows[1].fiscal_quarter, "FY");
        assert_eq!(rows[2].fiscal_quarter, "Q4");
    }

    #[test]
    fn sector_union_columns_round_trip() {
        // A bank row's union fields must survive write→read exactly; the
        // general rows in the same file keep them all None.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fundamentals_all.parquet");
        let mut rows = sample_rows();
        rows[0].sector_kind = "bank".into();
        rows[0].total_income = Some(120000.0);
        rows[0].interest_earned = Some(87182.5);
        rows[0].interest_expended = Some(45220.44);
        rows[0].net_interest_income = Some(41962.06);
        rows[0].gross_npa_pct = Some(1.33);
        rows[0].net_npa_pct = Some(0.43);
        rows[1].sector_kind = "insurance".into();
        rows[1].gross_premium_income = Some(27938.86);
        rows[1].net_premium_income = Some(27683.79);
        rows[1].benefits_paid = Some(16254.62);
        sort_rows(&mut rows);
        write_parquet(&path, &rows).unwrap();
        let back = read_parquet(&path).unwrap();
        assert_eq!(rows, back);
        let general: Vec<_> = back.iter().filter(|r| r.sector_kind == "general").collect();
        assert!(!general.is_empty());
        for g in general {
            assert!(g.interest_earned.is_none() && g.gross_premium_income.is_none());
        }
    }

    #[test]
    fn reading_pre_phase3_parquet_yields_null_sector_columns() {
        // Simulate the accumulated Phase-2 file (written before the sector
        // union existed) by projecting those columns away, then read it with
        // the current reader: absent column → None, never an error.
        use parquet::arrow::ArrowWriter;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fundamentals_all.parquet");
        let mut rows = sample_rows();
        rows[0].gross_npa_pct = Some(9.9); // would be dropped by projection
        sort_rows(&mut rows);
        let batch = to_batch(&rows).unwrap();
        let sector_cols: std::collections::HashSet<&str> = [
            "total_income", "interest_earned", "net_interest_income", "nim_pct",
            "interest_expended", "operating_expenses", "pre_provision_operating_profit",
            "provisions_and_contingencies", "gross_npa_pct", "net_npa_pct",
            "impairment_on_financial_instruments", "gross_stage3_pct",
            "gross_premium_income", "net_premium_income", "investment_income",
            "net_commission", "benefits_paid", "combined_ratio_pct", "solvency_ratio",
        ]
        .into_iter()
        .collect();
        let keep: Vec<usize> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !sector_cols.contains(f.name().as_str()))
            .map(|(i, _)| i)
            .collect();
        let old_batch = batch.project(&keep).unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, old_batch.schema(), None).unwrap();
        w.write(&old_batch).unwrap();
        w.close().unwrap();

        let back = read_parquet(&path).unwrap();
        assert_eq!(back.len(), rows.len());
        for r in &back {
            assert!(r.gross_npa_pct.is_none(), "absent column must read as None");
            assert!(r.total_income.is_none());
            assert!(r.solvency_ratio.is_none());
        }
        // Everything that WAS in the old schema still round-trips.
        assert_eq!(back[0].instrument_key, rows[0].instrument_key);
        assert_eq!(back[0].revenue, rows[0].revenue);
    }

    #[test]
    fn missing_file_reads_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_parquet(&dir.path().join("none.parquet")).unwrap().is_empty());
    }

    #[test]
    fn date_column_mirrors_as_of_for_the_manifest_machinery() {
        use arrow_array::cast::AsArray;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fundamentals_all.parquet");
        let mut rows = sample_rows();
        sort_rows(&mut rows);
        write_parquet(&path, &rows).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        let dates = batch.column_by_name("date").expect("date column").as_string::<i32>();
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(dates.value(i), r.as_of);
        }
    }
}
