{% set config = {
    "extract_type": "full"
} %}

select 
    country_id,
    country, 
    last_update
from    
    {{ source_table }}
