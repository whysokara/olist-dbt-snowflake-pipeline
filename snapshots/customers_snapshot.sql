{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_unique_id',
        strategy='check',
        check_cols=['city', 'state', 'zip_code_prefix']
    )
}}

select
    customer_unique_id,
    zip_code_prefix,
    city,
    state
from {{ ref('stg_customers_deduped') }}

{% endsnapshot %}