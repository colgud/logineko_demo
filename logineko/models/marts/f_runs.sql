{{ 
    config
    (
      materialized = "table",
      alias = "F_RUNS",
      schema = 'DWH',
      tags = ["FACT"]
    ) 
}}

with
data as
(
    select    f.date_sid
            , e2.equipment_sid as level_1_equipment_sid
            , f.equipment_sid
            , f.datetime   
            , lag(f.datetime) over (partition by e2.equipment_sid order by f.datetime) as datetime_prev
            , TIMESTAMPDIFF(second, lag(f.datetime) over (partition by e2.equipment_sid order by f.datetime), f.datetime) as duration
            , f.gpslongitude
            , f.gpslatitude
            , lag(f.gpslongitude) ignore nulls over (partition by e2.equipment_sid order by f.datetime) as gpslongitude_prev
            , lag(f.gpslatitude) ignore nulls over (partition by e2.equipment_sid order by f.datetime) as gpslatitude_prev    
    		, HAVERSINE(f.gpslatitude, f.gpslongitude, gpslatitude_prev, gpslongitude_prev) AS distance_km                      
            , f.speed
            , f.fuel_l_h
            , f.fuel_consumption
            , f.altitude
            , f.course 
            , e.business_key
            , e.type
            , e.source_system
            , e2.business_key as level_1_business_key
            , e2.type as level_1_type
            , e2.source_system as level_1_source_system
    from    {{ ref('f_sensor_raw') }} f
            join {{ ref('d_equipment') }} e on f.equipment_sid = e.equipment_sid and e.valid_to = '2100-12-31'
            join {{ ref('d_equipment') }} e2 on e.level_1_business_key = e2.business_key and e2.type = 'INTEGRATED' and e2.valid_to = '2100-12-31'
),
enum as
(
    select    f.*
            , sum(case 
                when f.duration > 3600 then 1
                else 0
              end) over (partition by f.level_1_equipment_sid order by f.datetime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as run_id   
    from    data f
),
rnk as 
(
    select  *, row_number() over (partition by level_1_equipment_sid, run_id order by datetime) as rn
    from    enum
),
divide as
(
    select  *, FLOOR((rn - 1) / 4000) + 1 as group_id
    from    rnk 
),
border as 
(
    select    level_1_equipment_sid
            , run_id
            , group_id
            , row_number() over (partition by level_1_equipment_sid, run_id order by group_id desc) as group_id_desc
            , count(*) as num
            , min(datetime) as datetime_min
            , max(datetime) as datetime_max 
    from    divide            
    group by  level_1_equipment_sid
            , run_id
            , group_id
),
result_1 as
(
    select    e.date_sid
            , e.level_1_equipment_sid as equipment_sid
            , e.datetime
            , b.datetime_min
            , b.datetime_max
            , e.duration
            , e.run_id
            , e.group_id
            , b.group_id_desc
            , case when e.datetime = b.datetime_min then e.datetime else null end as run_start
            , case when e.datetime = b.datetime_max then e.datetime else null end as run_end
            , case when e.datetime = b.datetime_min then e.gpslatitude else null end as gpslatitude_start
            , case when e.datetime = b.datetime_min then e.gpslongitude else null end as gpslongitude_start
            , case when e.datetime = b.datetime_max then e.gpslatitude else null end as gpslatitude_end
            , case when e.datetime = b.datetime_max then e.gpslongitude else null end as gpslongitude_end
            , ARRAY_CONSTRUCT(e.gpslatitude, e.gpslongitude) as point
            , e.gpslatitude
            , e.gpslongitude
            , e.distance_km
            , e.fuel_consumption
    from    divide e
            join border b on e.level_1_equipment_sid = b.level_1_equipment_sid and e.run_id = b.run_id and e.group_id = b.group_id
),
result as
(
    select    max(run_start)::date as date_sid
            , equipment_sid
            , run_id
            , group_id
            , group_id_desc
            , max(run_start) as run_start
            , max(run_end) as run_end
            , case when group_id = 1 then ARRAY_CONSTRUCT(max(gpslatitude_start), max(gpslongitude_start)) else null end as run_point_start
            , case when group_id_desc = 1 then ARRAY_CONSTRUCT(max(gpslatitude_end), max(gpslongitude_end)) else null end as run_point_end
            , case
                when count(*) >= 3 then calculate_convex_hull(array_agg(point))
                else null
              end as coverage_map 
            , count(*) as number_of_points
            , TIMESTAMPDIFF(second, max(run_start), max(run_end))/3600 as duration_h
            , sum(distance_km) as distance_km_traveled
            , sum(fuel_consumption) as fuel_used_litres
    from    result_1
    group by equipment_sid, run_id, group_id, group_id_desc
)
select    min(run_start)::date as date_sid
        , equipment_sid
        , run_id
        , min(run_start) as run_start
        , max(run_end) as run_end
        , array_agg(run_point_start) as run_point_start
        , array_agg(run_point_end) as run_point_end
        , case when ARRAY_SIZE(ARRAY_UNION_AGG(coverage_map)) >= 3 then calculate_convex_hull(ARRAY_UNION_AGG(coverage_map)) else ARRAY_UNION_AGG(coverage_map) end as coverage_map 
        , sum(number_of_points) as number_of_points
        , sum(duration_h) as duration_h
        , sum(distance_km_traveled) as distance_km_traveled
        , sum(fuel_used_litres) as fuel_used_litres
from    result
group by equipment_sid, run_id
order by equipment_sid, run_id
