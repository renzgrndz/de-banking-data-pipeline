{{ config(materialized='view') }}

with ranked as (
    SELECT
        v:id::string                 AS transaction_id,
        v:account_id::string         AS account_id,
        v:amount::float              AS amount,
        v:txn_type::string           AS transaction_type,
        v:related_account_id::string AS related_account_id,
        v:status::string             AS status,
        v:created_at::timestamp      AS transaction_time,
        CURRENT_TIMESTAMP            AS load_timestamp,
        row_number() over (
            partition by v:id::string
            order by v:created_at::timestamp desc
        ) as rn
    FROM {{ source('raw', 'transactions') }}
)

select
    transaction_id,
    account_id,
    amount,
    transaction_type,
    related_account_id,
    status,
    transaction_time,
    load_timestamp
from ranked
where rn = 1