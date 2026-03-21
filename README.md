# KL bus reliability tracker — architecture

## Stack
- **Ingestion:** Python poller + Airflow
- **Storage:** Google Cloud Storage (data lake) + BigQuery (data warehouse)
- **Transformation:** dbt
- **Provisioning:** Terraform
- **Dashboard:** Looker Studio

## Data flow

```mermaid
flowchart TD
    A1["data.gov.my API
    GTFS static zip
    rapid-bus-kl + mrtfeeder"]
    A2["data.gov.my API
    GTFS realtime
    vehicle positions every 30s"]

    subgraph TF["Terraform — infrastructure provisioning"]
      GCS["Google Cloud Storage
      raw/ partitioned by date"]
      BQR["BigQuery
      dataset: raw"]
      BQT["BigQuery
      dataset: transformed"]
    end

    subgraph AF["Airflow — orchestration"]
      DAG1["DAG 1 — static ingest
      daily @ 4am
      unzip → upload → load"]
      DAG2["DAG 2 — realtime ingest
      every 5 min
      batch snapshots → GCS → BQ"]
    end

    subgraph DBT["dbt — transformation"]
      STG["staging models
      clean types, filter bad trips"]
      INT["intermediate models
      derive headways
      flag ghost trips"]
      MRT["mart models
      pre-aggregated by route + hour"]
    end

    LS["Looker Studio
    KL bus reliability tracker
    Report 1 · Report 2"]

    A1 -->|"zip download"| DAG1
    A2 -->|"protobuf poll"| DAG2
    DAG1 -->|"CSV files"| GCS
    DAG2 -->|"parquet batches"| GCS
    GCS -->|"external tables"| BQR
    BQR --> STG
    STG --> INT
    INT --> MRT
    MRT --> BQT
    BQT -->|"direct connector"| LS
```

## Layer descriptions

**Google Cloud Storage** — raw landing zone. Static GTFS files land as CSVs, realtime snapshots land as parquet batched in 5-minute windows. Partitioned by date so historical data is queryable without scanning everything.

**BigQuery raw** — external tables pointing at GCS. No transformation, no data movement — just a queryable layer over the raw files.

**dbt staging** — light cleaning only. Rename columns to snake_case, cast types, filter out the ~2% of `rapid-bus-kl` trips flagged as problematic in the API docs.

**dbt intermediate** — the core logic. Join realtime vehicle positions against `stop_times.txt` to derive observed headways. Left join scheduled trips against observed positions to flag ghost trips.

**dbt mart** — pre-aggregated tables optimised for Looker Studio queries. One table per report, partitioned by date and clustered by route.

**Looker Studio** — connects directly to BigQuery mart tables. Global period filter drives both reports simultaneously.

## Known limitations

- Realtime historical data only available from poller start date. Prior period uses synthetic data generated from static schedule with injected noise.
- ~2% of `rapid-bus-kl` trips removed from `stop_times.txt` by the API provider due to data quality issues. These are filtered at the staging layer and noted in dbt model documentation.
- GTFS realtime feed provides vehicle positions only — trip updates and service alerts are not yet available for this operator.
