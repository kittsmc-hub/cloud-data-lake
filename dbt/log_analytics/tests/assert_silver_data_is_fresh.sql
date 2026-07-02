-- Singular test: fails the `dbt test` run if the most recent event in
-- silver is more than 2 hours old. This is the freshness SLA enforcement —
-- if the pipeline silently stops running, this test (not a human noticing
-- a stale dashboard) is what catches it.

select
    max(event_ts) as most_recent_event,
    now() - max(event_ts) as staleness
from {{ ref('stg_logs') }}
having now() - max(event_ts) > interval '2 hours'
