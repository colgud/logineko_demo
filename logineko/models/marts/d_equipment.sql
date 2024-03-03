{{ 
    config
    (
      materialized = "table",
      alias = "D_EQUIPMENT",
      schema = 'DWH',
      tags = ["DIMENSION"]
    ) 
}}

with
e as
(
    select distinct MD5('TELEMATICS|'||serialnumber) as equipment_sid, serialnumber as business_key, 'TELEMATICS' as source_system, 'INTEGRATED' as type from {{ ref('telematics_dump') }}
    union all
    select distinct MD5('FENDT|'||machineid) as equipment_sid, cast(machineid as varchar) as business_key, 'FENDT' as source_system, 'INTEGRATED' as type from {{ ref('fendt_gps_parsed') }}
    union all
    select distinct MD5('WIALON|'||unit_id) as equipment_sid, cast(unit_id as varchar) as business_key, 'WIALON' as source_system, '3RD PARTY' as type from {{ ref('wialon_dump') }}
),
m as
(
	select	*
	from	{{ ref('st_equipment_mapping_final') }}
)
select	  e.equipment_sid, e.business_key, e.source_system, e.type
		, coalesce(m.telemetry_id, e.business_key) as level_1_business_key
		, coalesce(m.wialon_vehicle_id, e.business_key) as level_2_business_key
		, cast('1900-01-01' as timestamp) as valid_from
		, cast('2100-12-31' as timestamp) as valid_to
from	e 
		left join m on e.business_key = m.wialon_vehicle_id