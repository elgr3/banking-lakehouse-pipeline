-- Silver: cours de change dédoublonnés et typés. EUR ajouté avec un taux de 1.
with typed as (
    select distinct
        try_cast(rate_date as date)                 as rate_date,
        {{ clean_currency('currency') }}            as currency,
        try_cast(rate_to_eur as decimal(18, 8))     as rate_to_eur
    from {{ ref('bronze_fx_rates') }}
),

eur as (
    select distinct rate_date, 'EUR' as currency, cast(1 as decimal(18, 8)) as rate_to_eur
    from typed
)

select * from typed where rate_date is not null and rate_to_eur > 0
union all
select * from eur
