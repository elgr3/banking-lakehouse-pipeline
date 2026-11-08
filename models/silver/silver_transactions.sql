{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}
-- Silver: transactions valides, dédoublonnées, converties en EUR.
-- Incrémental sur ingested_at : seuls les nouveaux lots sont traités, les
-- enregistrements tardifs (late-arriving) et les rejeux sont absorbés via unique_key.
with valid as (
    select *
    from {{ ref('int_transactions_validated') }}
    where rejection_reason is null
    {% if is_incremental() %}
      and ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
),

dedup as (
    select *
    from valid
    qualify row_number() over (partition by transaction_id order by ingested_at desc) = 1
)

select
    transaction_id,
    account_id,
    merchant_id,
    transaction_ts,
    cast(transaction_ts as date)                                as transaction_date,
    amount                                                      as amount_original,
    currency                                                    as currency_original,
    rate_to_eur,
    fx_rate_date,
    cast(round(amount * rate_to_eur, 2) as decimal(18, 2))      as amount_eur,
    channel,
    status,
    ingested_at,
    date_diff('day', transaction_ts, ingested_at) > {{ var('late_arrival_days') }} as is_late_arrival,
    _source_file,
    current_timestamp                                           as _processed_at
from dedup
