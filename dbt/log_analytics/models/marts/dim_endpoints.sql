-- Per-endpoint rollup: which routes get the most traffic and which are
-- the least reliable. Powers the "top endpoints" and "worst offenders"
-- panels on the dashboard.

with logs as (
    select * from {{ ref('stg_logs') }}
),

by_endpoint as (
    select
        endpoint,
        method,
        count(*)                                        as request_count,
        sum(case when is_server_error then 1 else 0 end) as server_error_count,
        round(
            sum(case when is_server_error then 1 else 0 end)::double / nullif(count(*), 0),
            4
        )                                                as server_error_rate,
        round(avg(latency_ms), 1)                        as avg_latency_ms,
        round(quantile_cont(latency_ms, 0.95), 1)         as p95_latency_ms
    from logs
    group by 1, 2
)

select
    *,
    rank() over (order by request_count desc) as traffic_rank
from by_endpoint
