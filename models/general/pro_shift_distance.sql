{{ config(
    materialized = "table"
) }}
WITH pro_lat AS (
SELECT * FROM (
  select
    id,
    case when position(',' in geocode)!=0 then substring(geocode,1,position(',' in geocode)-1)::real else null end as prolat,
    case when position(',' in geocode)!=0 then substring(geocode,position(',' in geocode)+1,length(geocode)-position(',' in geocode))::real else null end as prolong,
    inferred_regionmapping_id,
    regionmapping_id,
    date_created
    from iw_backend_db.backend_userprofile
) WHERE prolat IS NOT NULL AND inferred_regionmapping_id IS NOT NULL
)
SELECT
  a.shift_id,
  a.worker_id,
  MIN(ST_DistanceSphere(ST_Point(pl.latitude::real,pl.longitude::real),ST_Point(prolat,prolong))*0.000621371) AS dist 
FROM  
  "ml-ahan".fill_rate__pro_book  a LEFT JOIN
  iw_backend_db.business biz ON a.business_id = biz.id left join
  iw_backend_db.places_place pl on pl.id=biz.place_id LEFT JOIN
  pro_lat pro ON a.worker_id = pro.id AND a.booked_time >= pro.date_created
GROUP BY 1,2