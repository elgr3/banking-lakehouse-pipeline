-- Silver: un enregistrement par client (dernière version selon updated_at), colonnes typées.
with ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by try_cast(updated_at as timestamp) desc nulls last
        ) as _rn
    from {{ ref('bronze_customers') }}
    where customer_id is not null
)

select
    customer_id,
    trim(first_name)                                as first_name,
    trim(last_name)                                 as last_name,
    nullif(lower(trim(email)), '')                  as email,
    {{ parse_date('birth_date') }}                  as birth_date,
    upper(trim(country))                            as country,
    upper(trim(segment))                            as segment,
    try_cast(created_at as timestamp)               as created_at,
    try_cast(updated_at as timestamp)               as updated_at
from ranked
where _rn = 1
