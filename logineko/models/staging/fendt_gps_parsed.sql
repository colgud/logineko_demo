{{ 
    config
    (
      materialized = "table",
      alias = "FENDT_GPS_PARSED",
      schema = 'STAGE',
      tags = ["STAGE"]
    ) 
}}

SELECT
    id,
    json_data:machineId::STRING AS machineId,
    f.value:lng::FLOAT AS lng,
    f.value:lat::FLOAT AS lat,
    f.value:t::STRING AS t,
    to_timestamp(f.value:t::STRING) as datetime
FROM
    {{ ref('fendt_gps') }},
    LATERAL FLATTEN(input => json_data:route) f