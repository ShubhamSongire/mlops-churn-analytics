# Raw Data Storage Design

The raw layer stores ingested data in a structured but minimally transformed form.  To
support efficient ingestion and downstream processing the following
layout conventions are used:

* **Source partitioning** – Data is organised first by its origin.  There
  are separate top‑level folders for each source type, e.g. `source_csv` and
  `source_api`.  Additional sources (e.g. database dumps, SFTP imports)
  would appear as additional top‑level partitions.
* **Date partitioning** – Within each source folder data is stored in
  subdirectories labelled with the extraction date in `YYYYMMDD` format.
  This makes it easy to discover and process data from a particular
  ingestion run.
* **File naming** – Files under `source_csv/<date>/` retain their original
  names (`customers.csv`, `transactions.csv`, …).  Files under
  `source_api/<date>/` similarly retain their source names (e.g.
  `web_logs.jsonl`).  When ingested into the raw layer a timestamped
  prefix `ingested_<timestamp>_` is prepended to prevent name
  collisions across runs.

Example directory tree:

```
data/
  raw/
    source_csv/
      20250821/
        customers.csv
        transactions.csv
    source_api/
      20250821/
        web_logs.jsonl
    ingested_20250821_153000_customers.csv
    ingested_20250821_153000_transactions.csv
    ingested_20250821_153000_web_logs.jsonl
  logs/
    ingestion/
  validated/
    20250821/
  prepared/
  transformed/
```

Although this assignment uses a local filesystem, the same structure
would apply to cloud object stores (e.g., AWS S3 or GCS) with
appropriate bucket prefixes (e.g., `s3://company-data/raw/source_csv/…`).
