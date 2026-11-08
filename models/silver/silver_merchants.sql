-- Silver: commerçants, catégorie manquante complétée via le référentiel MCC (seed).
select
    m.merchant_id,
    trim(m.merchant_name)                           as merchant_name,
    cast(m.mcc as varchar)                          as mcc,
    coalesce(upper(trim(m.category)), ref.category, 'OTHER') as merchant_category,
    m.category is null                              as category_was_imputed,
    upper(trim(m.country))                          as country,
    try_cast(m.onboarded_at as date)                as onboarded_at
from {{ ref('bronze_merchants') }} m
left join {{ ref('mcc_categories') }} ref on ref.mcc = cast(m.mcc as varchar)
