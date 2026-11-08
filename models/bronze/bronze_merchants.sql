-- Bronze: copie fidèle de la source brute + métadonnées de chargement.
select
    *,
    current_timestamp as _loaded_at
from {{ source('raw', 'merchants') }}
