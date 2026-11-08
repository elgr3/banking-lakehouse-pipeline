-- Silver: comptes typés.
select
    account_id,
    customer_id,
    upper(trim(account_type))                       as account_type,
    {{ clean_currency('currency') }}                as currency,
    try_cast(opened_at as date)                     as opened_at,
    upper(trim(status))                             as status
from {{ ref('bronze_accounts') }}
where account_id is not null
