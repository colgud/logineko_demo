{{ config(schema = 'INGEST') }}

SELECT  *
FROM    {{ source('INGEST', 'WIALON_DUMP') }}
