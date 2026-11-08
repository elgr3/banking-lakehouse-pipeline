-- Gold: vue 360 client (profil, comptes, activité carte, alertes fraude).
with accounts as (
    select
        customer_id,
        count(*)                                            as n_accounts,
        count(*) filter (where status = 'ACTIVE')           as n_active_accounts,
        list_sort(list_distinct(list(account_type)))        as account_types
    from {{ ref('silver_accounts') }}
    group by customer_id
),

tx as (
    select
        a.customer_id,
        t.transaction_date,
        t.amount_eur,
        t.channel,
        m.merchant_category
    from {{ ref('silver_transactions') }} t
    join {{ ref('silver_accounts') }} a using (account_id)
    join {{ ref('silver_merchants') }} m using (merchant_id)
    where t.status = 'APPROVED'
),

activity as (
    select
        customer_id,
        count(*)                                            as approved_tx_count,
        sum(amount_eur)                                     as total_spend_eur,
        round(avg(amount_eur), 2)                           as avg_ticket_eur,
        min(transaction_date)                               as first_tx_date,
        max(transaction_date)                               as last_tx_date,
        count(*) filter (where channel = 'ECOM')            as ecom_tx_count
    from tx
    group by customer_id
),

fav as (
    select customer_id, arg_max(merchant_category, n) as favourite_category
    from (
        select customer_id, merchant_category, count(*) as n
        from tx
        group by customer_id, merchant_category
    )
    group by customer_id
),

alerts as (
    select
        customer_id,
        count(*)                                            as fraud_alerts,
        count(*) filter (where risk_level = 'HIGH')         as high_risk_alerts
    from {{ ref('gold_fraud_signals') }}
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.birth_date,
    date_diff('year', c.birth_date, date '2025-07-01')     as age_at_2025_07_01,
    c.country,
    c.segment,
    c.created_at                                            as customer_since,
    coalesce(acc.n_accounts, 0)                             as n_accounts,
    coalesce(acc.n_active_accounts, 0)                      as n_active_accounts,
    acc.account_types,
    coalesce(act.approved_tx_count, 0)                      as approved_tx_count,
    coalesce(act.total_spend_eur, 0)                        as total_spend_eur,
    act.avg_ticket_eur,
    act.first_tx_date,
    act.last_tx_date,
    round(coalesce(act.ecom_tx_count, 0) / nullif(act.approved_tx_count, 0), 4) as ecom_share,
    fav.favourite_category,
    coalesce(al.fraud_alerts, 0)                            as fraud_alerts,
    coalesce(al.high_risk_alerts, 0)                        as high_risk_alerts
from {{ ref('silver_customers') }} c
left join accounts acc using (customer_id)
left join activity act using (customer_id)
left join fav using (customer_id)
left join alerts al using (customer_id)
