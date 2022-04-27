{{ config(
    materialized = "table"
) }}
with biz AS (
SELECT
  bc.date_created as signup_date,
  biz.company_id,
  bc.name AS company_name,
  biz.id AS business_id,
  biz.name AS business_name,
  rm.id AS regionmapping_id,
  rm.displayname AS region_name,
  bt.display_name AS business_type,
  CASE
    WHEN biz.company_id IN (
      SELECT
        DISTINCT company_id
      FROM
        iw_backend_db.backend_gigpositionrate
      WHERE
        is_enabled = 1
    ) THEN 'fixed'
    ELSE 'flexible'
  END AS pricing_type,
  CASE
    WHEN 1 = 1
    AND biz.company_id NOT IN (
      2848, 8428,16207,13360,19951,20541,
      19737,19695,14368,21398,
      21341,18694,20744,15731,
      21921,21341,19643
    )
    AND bt.id != 5 THEN 'Hospitality'
    ELSE 'Light Industrial'
  END AS industry
FROM
  iw_backend_db.business biz
  LEFT JOIN iw_backend_db.backend_company bc ON biz.company_id = bc.id
  LEFT JOIN iw_backend_db.backend_regionmapping rm ON biz.regionmapping_id = rm.id
  LEFT JOIN iw_backend_db.backend_businesstype bt ON bc.business_type_reference = bt.id
),
base AS (
  SELECT
    a.shift_group_id,
    a.business_id, 
    a.business_name,
    a.biz_region_name,
    a.company_id,
    a.company_name,
    CASE WHEN DATE_PART(DOW,a.starts_at) IN (0,6) THEN 1 ELSE 0 END  AS weekend_gig,
    CASE WHEN EXTRACT(HOUR FROM a.starts_at) > 18 THEN 1 ELSE 0 END  AS night,
    DATEDIFF(hour, starts_at, ends_at) as shift_length,
    DATEDIFF(hour, starts_at, ends_at)*a.booking_applicant_rate_usd AS total_shift_wages,
    a.starts_at,
    a.ends_at,
    a.created_at,
    a.booking_applicant_rate_usd,  
    a.business_type,
    a.primary_industry,
    a.secondary_industry,
    biz.industry,
    biz.pricing_type,
    CASE WHEN a.booking_applicant_rate_usd < 15 THEN 1
      WHEN a.booking_applicant_rate_usd < 25 THEN 2
      WHEN a.booking_applicant_rate_usd < 40 THEN 3
      WHEN a.booking_applicant_rate_usd >=40 THEN 4 ELSE 5 END as wage_bucket_num,
    MAX(a.gig_position) as gig_position,
    AVG(a.booking_applicant_rate_usd) AS avg_wage,
    DATEDIFF(DAY,a.created_at,a.starts_at) AS created_days_to_shift,
    COUNT(DISTINCT a.shift_id) AS requested_pros,
    COUNT(DISTINCT CASE WHEN a.actual_starts_at IS NOT NULL AND is_cancelled =0 THEN a.worker_id ELSE NULL END) AS fill_0d,
    SUM(is_cancelled) AS bus_cancel 
FROM
    iw_backend_db.gigs_mv_lite a LEFT JOIN
    biz ON a.business_id = biz.business_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
) 

SELECT 
  aa.*,  
  (aa.requested_pros - aa.bus_cancel) AS final_requested,
  COUNT(DISTINCT ab.shift_group_id) AS mp_shift_groups,
  AVG(ab.booking_applicant_Rate_usd) AS mp_avg_wage,
  SUM(ab.requested_pros) as mp_total_requested
FROM 
  base AS aa LEFT JOIN 
  base AS ab ON aa.biz_region_name = ab.biz_region_name AND 
    aa.gig_position = ab.gig_position AND
    aa.created_at > ab.created_at AND DATE_TRUNC('week', aa.starts_at) = DATE_TRUNC('week', ab.starts_at)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27