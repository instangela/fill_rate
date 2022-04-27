{{ config(
    materialized = "table"
) }}
SELECT
  worker_id,
  shift_id,
  booked_time,
  first_shift_end,
  CASE WHEN first_shift_end IS NULL THEN 'F14'
    WHEN DATEDIFF(DAY,first_shift_end, booked_time) < 15 THEN 'F14'
    WHEN week_hours = 0 THEN 'inertia'
    WHEN week_hours > 50 OR hours_48h > 23 THEN 'overworked'
    WHEN week_hours >36 OR hours_48h > 15 THEN 'fulltime'
    ELSE 'multi_app' END as shift_style
FROM (
SELECT 
  a.worker_id,
  a.shift_group_id,
  a.shift_id,
  a.booked_time,
  a.booked_shift_n,
  a.n_completed_shifts,
  c.actual_ends_at AS first_shift_end,
  MAX(
  CASE  
    WHEN a.booked_shift_n <=5 THEN 1
    WHEN a.booked_shift_n <=25 THEN 2
    WHEN a.booked_shift_n <= 50 THEN 3
    WHEN a.booked_shift_n <=100 THEN 4
      ELSE 5 END) AS booked_shift_bucket,  
  SUM(b.shift_length) AS week_hours,
  SUM(CASE WHEN b.actual_ends_at > DATEADD(HOUR, -48, a.booked_time ) THEN b.shift_length ELSE 0 END) as hours_48h
FROM 
  "ml-ahan".fill_rate__pro_book a LEFT JOIN 
  "ml-ahan".fill_rate__pro_complete b ON a.worker_id = b.worker_id AND 
  b.actual_ends_at > DATEADD(HOUR, -168, a.booked_time)  AND b.actual_starts_at<= a.booked_time LEFT JOIN 
  "ml-ahan".fill_rate__pro_complete c ON a.worker_id = c.worker_id AND c.completed_shift_n = 1
GROUP BY 1,2,3,4,5,6,7
)