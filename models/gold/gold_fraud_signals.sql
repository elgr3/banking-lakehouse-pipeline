-- Gold: signaux de fraude à base de règles (scoring explicable, pas de ML).
-- Une ligne par transaction approuvée dont le score atteint le seuil d'alerte.
with tx as (
    select
        t.*,
        m.country                                   as merchant_country,
        m.merchant_category,
        c.customer_id,
        c.country                                   as customer_country
    from {{ ref('silver_transactions') }} t
    join {{ ref('silver_merchants') }} m using (merchant_id)
    join {{ ref('silver_accounts') }} a using (account_id)
    join {{ ref('silver_customers') }} c on c.customer_id = a.customer_id
    where t.status = 'APPROVED'
),

features as (
    select
        *,
        count(*) over (
            partition by account_id order by transaction_ts
            range between interval 10 minutes preceding and current row
        )                                                   as tx_last_10min,
        median(amount_eur) over (partition by account_id)   as account_median_eur
    from tx
),

signals as (
    select
        *,
        tx_last_10min >= 4                                                  as sig_velocity,
        amount_eur > 10 * account_median_eur and amount_eur > 500           as sig_amount_outlier,
        hour(transaction_ts) between 0 and 4
            and channel = 'ECOM' and amount_eur > 200                       as sig_night_ecom,
        merchant_country <> customer_country and amount_eur > 300           as sig_foreign_high_amount
    from features
),

scored as (
    select
        *,
        40 * sig_velocity::int
          + 35 * sig_amount_outlier::int
          + 15 * sig_night_ecom::int
          + 15 * sig_foreign_high_amount::int                               as fraud_score
    from signals
)

select
    transaction_id,
    account_id,
    customer_id,
    transaction_ts,
    amount_eur,
    merchant_id,
    merchant_category,
    merchant_country,
    customer_country,
    channel,
    tx_last_10min,
    round(account_median_eur, 2)                            as account_median_eur,
    sig_velocity,
    sig_amount_outlier,
    sig_night_ecom,
    sig_foreign_high_amount,
    fraud_score,
    case when fraud_score >= 70 then 'HIGH' else 'MEDIUM' end as risk_level
from scored
where fraud_score >= 35
