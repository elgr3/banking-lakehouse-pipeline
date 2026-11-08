-- Bronze: copie fidèle du flux de transactions + métadonnées de lignage.
-- La variable ingest_until permet de rejouer un chargement initial puis un incrémental.
select
    transaction_id,
    account_id,
    merchant_id,
    transaction_ts,
    amount,
    currency,
    channel,
    status,
    ingested_at,
    regexp_extract(replace(filename, '\', '/'), '[^/]+$') as _source_file,
    current_timestamp as _loaded_at
from {{ source('raw', 'transactions') }}
where try_cast(ingested_at as timestamp) <= cast('{{ var("ingest_until") }}' as timestamp)
