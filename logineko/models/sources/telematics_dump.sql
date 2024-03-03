{{ config(schema = 'INGEST') }}

SELECT  *
FROM    {{ source('INGEST', 'TELEMATICS_DUMP') }}
