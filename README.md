# 12-aws-reliability-security-platform

A reference data platform blueprint with pragmatic governance: quality checks, lineage, access controls, and CI for data assets.

Focus: platform


## The top pains this repo addresses
1) Shipping fast without compromising security—policy-as-code, least privilege, secrets hygiene, and audit-ready evidence collection.
2) Keeping production stable while the system scales—reducing incident frequency, improving MTTR, and building predictable operations (SLOs/runbooks/on-call hygiene).
3) Building a data platform people trust—reliable pipelines, clear ownership, data quality checks, and governance that scales without slowing delivery.

## Quick demo (local)
```bash
make demo
make test
```

What you get:
- a tiny deterministic pipeline (CSV → processed artifact)
- schema validation that runs without network dependencies
- a basic “trust contract” (docs + tests + CI)

Optional (production-style output): install dependencies and emit Parquet:
```bash
make setup
. .venv/bin/activate && python pipelines/pipeline.py --format parquet
```

## Tests (two modes)
This repository supports exactly two test modes via `TEST_MODE`:

- `demo`: runs fast, offline tests against local fixtures only.
- `production`: runs integration checks that require optional dependencies and explicit opt-in.

Run demo tests:
```bash
TEST_MODE=demo python3 tests/run_tests.py
```

Run production tests (guarded):
```bash
TEST_MODE=production PRODUCTION_TESTS_CONFIRM=1 python3 tests/run_tests.py
```

## Sponsorship and authorship
Sponsored by:
CloudForgeLabs  
https://cloudforgelabs.ainextstudios.com/  
support@ainextstudios.com

Built by:
Freddy D. Alvarez  
https://www.linkedin.com/in/freddy-daniel-alvarez/

For job opportunities, contact:
it.freddy.alvarez@gmail.com

## License
Personal/non-commercial use is free. Commercial use requires permission (paid license).
See `LICENSE` and `COMMERCIAL_LICENSE.md` for details. For commercial licensing, contact: `it.freddy.alvarez@gmail.com`.
Note: this is a non-commercial license and is not OSI-approved; GitHub may not classify it as a standard open-source license.
