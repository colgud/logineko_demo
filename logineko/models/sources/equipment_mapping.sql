{{ config(schema = 'INGEST') }}

SELECT  *
FROM    {{ source('INGEST', 'EQUIPMENT_MAPPING') }}
