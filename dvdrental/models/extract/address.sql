{% set config = {
    "extract_type": "incremental",
    "incremental_column": "address_id",
    "key_columns": ["address_id"]
} %}

select 
    address_id, 
    address,
    address2, 
    district,
    city_id, 
    postal_code, 
    phone,
    last_update 
from 
    {{ source_table }}

{% if is_incremental %}
    where address_id > {{ incremental_value }}
{% endif %}
