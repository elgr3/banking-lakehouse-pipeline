-- Silver: quarantaine des lignes rejetées avec leur motif (auditabilité).
select
    transaction_id,
    account_id,
    merchant_id,
    transaction_ts_raw,
    amount,
    currency,
    ingested_at,
    _source_file,
    rejection_reason
from {{ ref('int_transactions_validated') }}
where rejection_reason is not null
