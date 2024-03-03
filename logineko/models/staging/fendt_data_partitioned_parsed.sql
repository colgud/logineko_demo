{{ 
    config
    (
      materialized = "table",
      alias = "FENDT_DATA_PARTITIONED_PARSED",
      schema = 'STAGE',
      tags = ["STAGE"]
    ) 
}}

SELECT
    id,
    json_data:machineId::STRING AS machineId,
    json_data:count::STRING AS total_count,
    d.value:unit::STRING AS unit,
    d.value:count::STRING AS data_count,
    d.value:signalGroup::STRING AS signalGroup,
    d.value:type::STRING AS type,
    v.value:value::FLOAT AS value,
    v.value:timestamp::STRING AS timestamp,
    to_timestamp(v.value:timestamp::STRING) as datetime    
FROM
    {{ ref('fendt_data_partitioned') }},
    LATERAL FLATTEN(input => json_data:datas) d,
    LATERAL FLATTEN(input => d.value:values) v