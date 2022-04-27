{{ config(
    materialized = "table"
) }}
WITH bookings AS (

SELECT 
  worker_id,
  CAST(booked_time AS DATE) AS booked_date,
  SUM(1) AS shifts
FROM 
  "ml-ahan".fill_rate__pro_book
GROUP BY 1,2 ORDER BY 1,2
)

select  
  a.worker_id,
  a.shift_id,
  a.booked_time,
  b.shifts AS same_day_booked_shifts,
  COUNT(DISTINCT c.booked_date) AS n_days_shift_book,
  AVG(CASE WHEN c.shifts IS NULL THEN 0 ELSE 1.0*c.shifts END) AS avg_shift_booked
from   
  "ml-ahan".fill_rate__pro_book a LEFT JOIN 
  bookings b ON a.worker_id = b.worker_id AND b.booked_date = CAST(a.booked_time AS DATE) LEFT JOIN 
  bookings c ON a.worker_id = c.worker_id AND c.booked_date < CAST(a.booked_time AS DATE) AND c.booked_date >= DATEADD(day,-28,a.booked_time )
GROUP BY 1,2,3,4