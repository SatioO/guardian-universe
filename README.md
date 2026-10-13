# guardian-universe

Serverless EOD reference-data producer for the traderview app. Publishes versioned,
checksummed reference lists over CDN for the app to consume: the instrument list,
index constituents, sector/classification, fundamentals, the market calendar,
corporate actions, CAS eligibility and F&O freeze limits.

It still fetches and validates NSE's daily bhavcopy, because the instrument list is
built from it, but since 2026-10-13 it no longer publishes prices: the app builds
price history on the trader's device. See RUNBOOK.md, "Price datasets retired".

- Pipeline package: `pipeline/`
- Design spec: `docs/superpowers/specs/2026-07-04-scanner-data-pipeline-design.md`
- P0 plan: `docs/superpowers/plans/2026-07-04-scanner-data-pipeline-p0-producer.md`
