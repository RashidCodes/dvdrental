{% set config = {
    "extract_type": "full", 
    "incremental_column": "orderdate"
} %}

select 
    orderid, 
    orderdate,
    customerid, 
    netamount,
    tax, 
    totalamount
from 
    {{ source_table }}

{% if is_incremental %}
    where orderdate > '{{ incremental_value }}'
{% endif %}

