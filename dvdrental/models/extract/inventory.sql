{% set config = {
    "extract_type": "full"
} %}

select 
    inventory_id, 
    film_id,
    store_id, 
    last_update 
from 
    {{ source_table }}
