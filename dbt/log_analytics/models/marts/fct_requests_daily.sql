-- Daily traffic + reliability facts. This is what powers the dashboard's
-- headline metrics: request volume, error rate, and p50/p95 latency by day.

with logs as (
    select * from {{ ref('stg_logs') }}
),

daily as (
    select
        event_date,
        count(*)                                            as request_count,
        count(distinct user_id)                              as unique_users,
        sum(case when is_server_error then 1 else 0 end)     as server_error_count,
        sum(case when is_client_error then 1 else 0 end)     as client_error_count,
        round(
            sum(case when is_server_error then 1 else 0 end)::double / nullif(count(*), 0),
            4
        )                                                    as server_error_rate,
        round(avg(latency_ms), 1)                            as avg_latency_ms,
        round(median(latency_ms), 1)                         as p50_latency_ms,
        round(quantile_cont(latency_ms, 0.95), 1)             as p95_latency_ms,
        round(quantile_cont(latency_ms, 0.99), 1)             as p99_latency_ms
    from logs
    group by 1
)

select * from daily
order by event_date
