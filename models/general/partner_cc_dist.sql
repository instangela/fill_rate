SELECT 
  a.id AS business_id,
  a.name,
  b.latitude,
  b.longitude,
  b.regionmapping_id,
  c.geocode,
  c.name AS bus_region_name,
  MIN(ST_DistanceSphere(ST_Point(case when position(',' in c.geocode)!=0 then substring(c.geocode,1,position(',' in c.geocode)-1)::real else null end,
    CASE WHEN b.regionmapping_id = 74 THEN -112.0741417 
      WHEN position(',' in c.geocode)!=0 then substring(c.geocode,position(',' in c.geocode)+1,length(c.geocode)-position(',' in c.geocode))::real else null end),
    ST_Point(b.latitude,b.longitude))*0.000621371) AS dist 
FROM
  iw_backend_db.business a  LEFT JOIN   
  iw_backend_db.places_place b ON a.place_id = b.id LEFT JOIN
  iw_backend_db.backend_regionmapping c ON b.regionmapping_id = c.id AND c.geocode IS NOT NULL
GROUP BY 1,2,3,4,5,6,7