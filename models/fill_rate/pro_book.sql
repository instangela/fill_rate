{{ config(
    materialized = "table"
) }}

WITH bookings AS (
SELECT 
  event, 
  shift_id, 
  created_at, 
  worker_id,
  RANK() OVER (PARTITION BY worker_id ORDER BY created_at) AS booked_shift_n, 
  RANK() OVER (PARTITION BY worker_id, shift_id ORDER BY created_at) AS shiftid_booked_n
FROM 
  iw_backend_db.backend_workerlog 
WHERE 
  event = 9 
GROUP BY 1,2,3,4),
cancels AS (
SELECT
  event, 
  shift_id, 
  created_at, 
  id, 
  worker_id,
  RANK() OVER (PARTITION BY worker_id, shift_id ORDER BY created_at) AS shiftid_booked_n
FROM 
  iw_backend_db.backend_workerlog 
WHERE 
  event = 10
GROUP BY 1,2,3,4,5),
shifts_completed AS (
select 
  worker_id,
  actual_starts_at,
  id AS shift_id,
  RANK() OVER (PARTITION BY worker_id ORDER BY actual_starts_at) as completed_shift_n
from 
   iw_backend_db.backend_shift 
where 
  is_cancelled is false and actual_starts_at < getdate() 
group by 1,2,3),
defect AS (
select 
  wl.created_at, 
  wl.shift_id, 
  wl.worker_id, 
  gvl.starts_at,
  RANK() OVER (PARTITION BY wl.worker_id ORDER BY wl.created_at) AS defect_n
from 
  iw_backend_db.backend_workerlog wl left join 
  iw_backend_db.gigs_view_lite gvl on gvl.shift_id = wl.shift_id
where 
  event in (15,43,26) and 
  DATEDIFF(HOUR, wl.created_at, gvl.starts_at) < 25
GROUP BY 1,2,3,4
)

SELECT 
  gvl.shift_group_id,
  gvl.shift_id,
  gvl.business_id,
  gvl.business_name,
  gvl.gig_position,
  gvl.biz_region_name,
  book.worker_id,
  gvl.starts_at AS shift_starttime,
  gvl.ends_at AS shift_enddate,
  DATEDIFF(hour, gvl.starts_at, gvl.ends_at) as shift_length,
  book.created_at AS booked_time,
  cancels.created_at AS pro_cancel_time,
  CASE WHEN DATEDIFF(HOUR, cancels.created_at, gvl.starts_at) < 25 THEN 1 ELSE 0 END as urgent_defect_shift,
  book.booked_shift_n,
  CASE WHEN gvl.booking_applicant_rate_usd < 15 THEN 1
      WHEN gvl.booking_applicant_rate_usd < 25 THEN 2
      WHEN gvl.booking_applicant_rate_usd < 40 THEN 3
      WHEN gvl.booking_applicant_rate_usd >=40 THEN 4 ELSE 5 END as wage_bucket_num,
  gvl.booking_applicant_rate_usd,
  MAX(sc.actual_starts_at) AS last_completed_shift,
  MIN(sc.actual_starts_at) AS first_completed_shift,
  MAX(CASE WHEN sc.completed_shift_n IS NULL THEN 0 ELSE sc.completed_shift_n END) as n_completed_shifts,
  MAX(CASE WHEN df.defect_n IS NULL THEN 0 ELSE df.defect_n END) AS n_defects,
  MAX(df.created_at) AS last_defect_time
from 
  iw_backend_db.gigs_mv_lite gvl LEFT JOIN
  bookings book ON gvl.shift_id = book.shift_id LEFT JOIN --ONLY shift_id JOIN b/c worker_id who books not necessarily works it 
  cancels ON book.worker_id = cancels.worker_id AND gvl.shift_id = cancels.shift_id AND cancels.shiftid_booked_n = book.shiftid_booked_n LEFT JOIN --JOIN need to be on book.worker_id
  shifts_completed sc ON book.worker_id = sc.worker_id AND book.created_at > sc.actual_starts_at LEFT JOIN 
  defect df ON book.worker_id = df.worker_id AND book.created_at > df.created_at AND gvl.shift_id <> df.shift_id LEFT JOIN 
  defect df2 ON book.worker_id = df2.worker_id AND gvl.shift_id = df2.shift_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
ORDER BY 3,5