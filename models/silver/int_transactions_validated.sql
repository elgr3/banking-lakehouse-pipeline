{{ config(materialized='view') }}
-- Silver (intermédiaire): parsing + règles de qualité. Chaque ligne brute reçoit
-- soit un motif de rejet, soit null si elle est exploitable. Conversion EUR via
-- ASOF JOIN sur le dernier cours publié à la date de l'opération.
with parsed as (
    select
        nullif(trim(transaction_id), '')            as transaction_id,
        nullif(trim(account_id), '')                as account_id,
        nullif(trim(merchant_id), '')               as merchant_id,
        transaction_ts                              as transaction_ts_raw,
        {{ parse_timestamp('transaction_ts') }}     as transaction_ts,
        {{ parse_amount('amount') }}                as amount,
        {{ clean_currency('currency') }}            as currency,
        upper(trim(channel))                        as channel,
        upper(trim(status))                         as status,
        try_cast(ingested_at as timestamp)          as ingested_at,
        _source_file
    from {{ ref('bronze_transactions') }}
),

with_fx as (
    select
        p.*,
        fx.rate_to_eur,
        fx.rate_date                                as fx_rate_date
    from parsed p
    asof left join {{ ref('silver_fx_rates') }} fx
        on fx.currency = p.currency
        and cast(p.transaction_ts as date) >= fx.rate_date
),

flagged as (
    select
        f.*,
        case
            when f.transaction_id is null                         then 'missing_transaction_id'
            when f.account_id is null                             then 'missing_account_id'
            when a.account_id is null                             then 'unknown_account'
            when f.merchant_id is null or m.merchant_id is null   then 'unknown_merchant'
            when f.transaction_ts is null                         then 'invalid_timestamp'
            when f.transaction_ts > f.ingested_at                 then 'event_after_ingestion'
            when f.amount is null or f.amount <= 0                then 'invalid_amount'
            when f.currency not in (select distinct currency from {{ ref('silver_fx_rates') }})
                                                                  then 'unknown_currency'
            when f.rate_to_eur is null                            then 'missing_fx_rate'
        end as rejection_reason
    from with_fx f
    left join {{ ref('silver_accounts') }} a on a.account_id = f.account_id
    left join {{ ref('silver_merchants') }} m on m.merchant_id = f.merchant_id
)

select * from flagged
