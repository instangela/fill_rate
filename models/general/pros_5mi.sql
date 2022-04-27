{{ config(
    materialized = "table"
) }}

SELECT
  a.business_id,
  a.shift_group_id,
  a.created_at,
  COUNT(DISTINCT CASE WHEN pro.zipcode IS NOT NULL THEN user_id ELSE NULL END) AS pro_5mi
 -- SUM(CASE WHEN ST_DistanceSphere(ST_Point(pl.latitude::real,pl.longitude::real),ST_Point(prolat,prolong))*0.000621371 <=5 THEN 1 ELSE 0 END) AS pro_5mi
 -- SUM(CASE WHEN ST_DistanceSphere(ST_Point(pl.latitude::real,pl.longitude::real),ST_Point(prolat,prolong))*0.000621371 <=10 THEN 1 ELSE 0 END) AS pro_10mi
FROM
  "ml-ahan".fill_rate__train_data a LEFT JOIN
  iw_backend_db.business biz ON a.business_id = biz.id left join 
  iw_backend_db.places_place pl on pl.id=biz.place_id LEFT JOIN  
  "ml-ahan".fill_rate__features zip ON pl.zipcode = zip.zipcode LEFT JOIN 
  iw_backend_db.backend_userprofile pro ON zip.zipcode_5mi = pro.zipcode AND a.created_at > pro.date_created
GROUP BY 1,2,3