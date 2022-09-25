{% set config = {
    "extract_type": "incremental",
    "incremental_column": "customer_id",
    "key_columns": ["customer_id"]
} %}

select 
    customer_id, 
    store_id, 
    first_name,
    last_name, 
    email, 
    address_id, 
    activebool, 
    create_date, 
    last_update, 
    active 
from 
    {{ source_table }}

{% if is_incremental %} 
   where customer_id > {{ incremental_value }}
{% endif %}
