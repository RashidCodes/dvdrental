{% set config = {
    "extract_type": "full"
} %}

select 
    city_id, 
    city, 
    country_id, 
    last_update 
from 
    {{ source_table }}
