{{ config(schema = 'INGEST') }}

SELECT  *
FROM    {{ source('INGEST', 'FENDT_DATA_PARTITIONED') }}
