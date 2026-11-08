{# Parse a free-text timestamp coming from heterogeneous source systems. #}
{% macro parse_timestamp(col) -%}
    coalesce(
        try_strptime(trim({{ col }}), '%Y-%m-%dT%H:%M:%S'),
        try_strptime(trim({{ col }}), '%Y-%m-%d %H:%M:%S'),
        try_strptime(trim({{ col }}), '%d/%m/%Y %H:%M')
    )
{%- endmacro %}

{# Parse a free-text date (ISO or French dd/mm/yyyy). #}
{% macro parse_date(col) -%}
    cast(coalesce(
        try_strptime(trim({{ col }}), '%Y-%m-%d'),
        try_strptime(trim({{ col }}), '%d/%m/%Y')
    ) as date)
{%- endmacro %}

{# Parse an amount that may use a French decimal comma. #}
{% macro parse_amount(col) -%}
    try_cast(replace(trim({{ col }}), ',', '.') as decimal(18, 2))
{%- endmacro %}

{# Normalise an ISO-4217 currency code. #}
{% macro clean_currency(col) -%}
    nullif(upper(trim({{ col }})), '')
{%- endmacro %}
