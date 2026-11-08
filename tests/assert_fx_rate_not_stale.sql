-- Le cours appliqué ne doit pas dater de plus de 4 jours (week-end + éventuel jour férié).
select transaction_id, transaction_date, fx_rate_date
from {{ ref('silver_transactions') }}
where date_diff('day', fx_rate_date, transaction_date) > 4
