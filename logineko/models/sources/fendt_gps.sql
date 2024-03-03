{{ config(schema = 'INGEST') }}

SELECT  *
FROM    {{ source('INGEST', 'FENDT_GPS') }}
