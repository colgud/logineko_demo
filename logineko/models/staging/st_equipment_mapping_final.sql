{{ 
    config
    (
      materialized = "table",
      alias = "ST_EQUIPMENT_MAPPING_FINAL",
      schema = 'STAGE',
      tags = ["STAGE"]
    ) 
}}

select  * 
from    {{ ref('equipment_mapping') }}
where   telemetry_id <> ''
  and   telemetry_id <> 'A6002059'