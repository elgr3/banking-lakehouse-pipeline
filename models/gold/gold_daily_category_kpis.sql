-- Gold: KPIs quotidiens par catégorie de commerçant.
select
    t.transaction_date,
    m.merchant_category,
    count(*)                                                            as tx_count,
    count(*) filter (where t.status = 'APPROVED')                       as approved_count,
    count(*) filter (where t.status = 'DECLINED')                       as declined_count,
    round(count(*) filter (where t.status = 'DECLINED') / count(*), 4)  as decline_rate,
    coalesce(sum(t.amount_eur) filter (where t.status = 'APPROVED'), 0) as approved_amount_eur,
    round(avg(t.amount_eur) filter (where t.status = 'APPROVED'), 2)    as avg_ticket_eur,
    count(distinct t.account_id)                                        as active_accounts,
    count(*) filter (where t.is_late_arrival)                           as late_arrivals
from {{ ref('silver_transactions') }} t
join {{ ref('silver_merchants') }} m using (merchant_id)
group by all
