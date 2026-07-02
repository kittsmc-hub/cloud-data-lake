-- Staging layer: 1:1 with the silver Delta table, but with dbt-managed
-- naming/typing conventions so every downstream mart builds on a stable
-- contract instead of reaching into the raw source directly.

with source as (
    select * from {{ source('silver', 'events') }}
),

renamed as (
    select
        event_id,
        cast(event_time as timestamp)      as event_ts,
        date_trunc('day', event_time)      as event_date,
        endpoint,
        method,
        status_code,
        case when status_code >= 500 then true else false end as is_server_error,
        case when status_code >= 400 and status_code < 500 then true else false end as is_client_error,
        latency_ms,
        user_id,
        country,
        ingest_batch_id,
        user_agent,          -- nullable: only present on batches from schema_version >= 2
        session_id,          -- nullable: only present on batches from schema_version >= 3
        is_authenticated,    -- nullable: only present on batches from schema_version >= 3
        _ingested_at
    from source
)

select * from renamed
