-- La conversion EUR doit être reproductible à partir du montant d'origine et du cours appliqué.
select transaction_id, amount_original, rate_to_eur, amount_eur
from {{ ref('silver_transactions') }}
where abs(amount_eur - round(amount_original * rate_to_eur, 2)) > 0.01
