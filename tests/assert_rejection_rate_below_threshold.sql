-- Garde-fou qualité : le taux de rejet (hors doublons) ne doit pas dépasser max_rejection_rate.
with r as (select count(*) as rejected from {{ ref('silver_transactions_rejected') }}),
     b as (select count(*) as raw_rows from {{ ref('bronze_transactions') }})
select rejected, raw_rows, rejected / raw_rows as rejection_rate
from r, b
where rejected / raw_rows > {{ var('max_rejection_rate') }}
