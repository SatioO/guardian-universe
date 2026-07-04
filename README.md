# guardian-universe

Serverless EOD market-data producer for the traderview scanner. Fetches NSE/BSE
bhavcopy, validates + normalizes it, stores year-partitioned Parquet, and publishes
versioned, checksummed artifacts over CDN for the app to consume.

- Pipeline package: `pipeline/`
- Design spec: `docs/superpowers/specs/2026-07-04-scanner-data-pipeline-design.md`
- P0 plan: `docs/superpowers/plans/2026-07-04-scanner-data-pipeline-p0-producer.md`
