{{ 
    config
    (
      materialized = "table",
      alias = "F_SENSOR_RAW",
      schema = 'DWH',
      tags = ["FACT"]
    ) 
}}

with
fendt_gps as
(
	select 	machineid, datetime, lng, lat
	from 	{{ ref('fendt_gps_parsed') }}
	where	lng is not null
	  and	lat is not null
),
fendt_fuel as
(
	select 	machineid, datetime, value, row_number() over (partition by machineid, datetime order by 1) as rn
	from 	{{ ref('fendt_data_partitioned_parsed') }}
	where	type = 'FuelRate'
	  and	value is not null
),
fendt_speed as 
(
	select 	machineid, datetime, value, row_number() over (partition by machineid, datetime order by 1) as rn
	from 	{{ ref('fendt_data_partitioned_parsed') }}
	where	type = 'WheelBasedVehicleSpeed'
	  and	value is not null
)
select 	  datetime_converted::date as date_sid
		, MD5('TELEMATICS|'||serialnumber) as equipment_sid
		, datetime_converted as datetime
		, gpslongitude 
		, gpslatitude 
		, speedgearbox_km_h as speed
		, fuelconsumption_l_h as fuel_l_h
        , case
            when TIMESTAMPDIFF(second, lag(datetime_converted) over (partition by serialnumber ORDER BY datetime_converted), datetime_converted) <= 30 then TIMESTAMPDIFF(second, lag(datetime_converted) over (partition by serialnumber ORDER BY datetime_converted), datetime_converted)/3600 * fuelconsumption_l_h
            else 0
          end AS fuel_consumption
		, cast(null as numeric) as altitude
		, cast(null as numeric) as course
from 	{{ ref('telematics_dump') }}
where   gpslongitude is not null
  and   gpslatitude is not null
union all
select	  datetime_converted::date as date_sid
		, MD5('WIALON|'||unit_id) as equipment_sid
		, datetime_converted as datetime
		, gpslongitude 
		, gpslatitude 
		, speed
		, cast(null as numeric) as fuel_l_h
        , cast(null as numeric) as fuel_consumption
		, altitude 
		, course 
from	{{ ref('wialon_dump') }}
where   gpslongitude is not null
  and   gpslatitude is not null
union all
select	  fendt_gps.datetime::date as date_sid
		, MD5('FENDT|'||fendt_gps.machineid) as equipment_sid
		, fendt_gps.datetime 
		, fendt_gps.lng as gpslongitude 
		, fendt_gps.lat as gpslatitude 
		, fendt_speed.value as speed
		, fendt_fuel.value as fuel_l_h
        , case
            when TIMESTAMPDIFF(second, lag(fendt_gps.datetime) over (partition by fendt_gps.machineid ORDER BY fendt_gps.datetime), fendt_gps.datetime) <= 30 then TIMESTAMPDIFF(second, lag(fendt_gps.datetime) over (partition by fendt_gps.machineid ORDER BY fendt_gps.datetime), fendt_gps.datetime)/3600 * fendt_fuel.value
            else 0
          end AS fuel_consumption
		, cast(null as numeric) as altitude 
		, cast(null as numeric) as course 
from	fendt_gps
		left join fendt_fuel on fendt_gps.machineid = fendt_fuel.machineid and fendt_gps.datetime = fendt_fuel.datetime and fendt_fuel.rn = 1
		left join fendt_speed on fendt_gps.machineid = fendt_speed.machineid and fendt_gps.datetime = fendt_speed.datetime and fendt_speed.rn = 1