-- Réconciliation : volumes et montants approuvés en gold doivent égaler ceux de silver.
with g as (
    select sum(approved_amount_eur) as total, sum(tx_count) as n
    from {{ ref('gold_daily_category_kpis') }}
),
s as (
    select sum(amount_eur) filter (where status = 'APPROVED') as total, count(*) as n
    from {{ ref('silver_transactions') }}
)
select g.total as gold_total, s.total as silver_total, g.n as gold_n, s.n as silver_n
from g, s
where abs(g.total - s.total) > 0.01 or g.n <> s.n
