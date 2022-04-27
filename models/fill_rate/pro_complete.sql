{{ config(
    materialized = "table"
) }}

select 
  worker_id,
  actual_starts_at,
  actual_ends_at,
  DATEDIFF(HOUR, actual_starts_at, actual_ends_at) AS shift_length,
  id AS shift_id,
  RANK() OVER (PARTITION BY worker_id ORDER BY actual_starts_at) as completed_shift_n
from 
   iw_backend_db.backend_shift 
where 
  is_cancelled is false and actual_starts_at < getdate() 
group by 1,2,3,4,5