SELECT
    (CURRENT_DATE - 1) AS ds
    , gt.business_id AS ID_business_id
    , bwl.worker_id AS ID_worker_id
    , bwl.shift_id AS ID_shift_id
    , bwl.created_at AS TS_event_created_at
    , bwl.recorded_at AS TS_event_recorded_at
    , bwl.event AS MC_event
    -- Pro Shift Features
    , bs.is_shift_lead AS B_is_shift_lead
    , sg.booking_applicant_rate_usd AS RV_FLOAT_booking_applicant_rate_usd
    , sg.current_applicant_rate_usd AS RV_FLOAT_current_applicant_rate_usd
    , ((DATEDIFF(MINUTE, sg.starts_at, sg.ends_at) - coalesce(sg.break_length, 0)) / 60.0) * sg.booking_applicant_rate_usd AS RV_FLOAT_pro_booking_rate_shift_earning
    , ((DATEDIFF(MINUTE, sg.starts_at, sg.ends_at) - coalesce(sg.break_length, 0)) / 60.0) * sg.current_applicant_rate_usd AS RV_FLOAT_pro_current_rate_shift_earning
    , DATEDIFF(DAY, bwl.recorded_at, sg.starts_at) AS RV_INT_days_between_shift_and_worker_assigned
    -- Shift Features
    , gt.background_check_required AS B_is_background_check_required
    , sg.is_break_paid AS B_is_break_paid
    , gt.has_free_meals AS B_is_free_food_provided
    , gt.is_long_term_assignment AS B_is_long_term_shift
    , gt.w2_employees_only AS B_is_w2_required
    , gt.has_parking AS MC_is_parking_available
    , EXTRACT(DOW FROM sg.starts_at AT TIME ZONE 'UTC' AT TIME ZONE pp.timezone) AS MC_local_day_of_week
    , EXTRACT(HOUR FROM sg.starts_at AT TIME ZONE 'UTC' AT TIME ZONE pp.timezone) AS MC_local_start_hour
    , gt.position_fk_id AS MC_position_id
    , ((DATEDIFF(MINUTE, sg.starts_at, sg.ends_at) - coalesce(sg.break_length, 0)) / 60.0) AS RV_FLOAT_og_shift_duration_hours
    , ((DATEDIFF(MINUTE, sg.starts_at, sg.ends_at) - coalesce(sg.break_length, 0)) / 60.0) * bs.business_rate_usd AS RV_FLOAT_og_total_shift_amount
    , bs.booking_fee_usd AS RV_FLOAT_shift_booking_fee
    , bs.business_rate_usd AS RV_FLOAT_shift_hourly_rate
    , sg.break_length AS RV_INT_break_length
    , sg.starts_at AT TIME ZONE 'UTC' AT TIME ZONE pp.timezone AS TS_local_starts_at
    , sg.starts_at AS TS_shift_group_starts_at
FROM
    {{ source("instawork-dw-backend", "backend_workerlog") }} bwl
    LEFT JOIN {{ source("instawork-dw-backend", "backend_shift") }} bs ON bwl.shift_id = bs.id
    LEFT JOIN {{ source("instawork-dw-backend", "backend_shiftgroup") }} sg ON sg.id = bs.shift_group_id
    LEFT JOIN {{ source("instawork-dw-backend", "backend_gigtemplate") }} gt ON gt.id = bs.gig_id
    LEFT JOIN {{ source("instawork-dw-backend", "business") }} business ON gt.business_id = business.id
    LEFT JOIN {{ source("instawork-dw-backend", "places_place") }} pp ON business.place_id = pp.id
WHERE
    bwl.event IN (9)  -- WORKER_ASSIGN
    AND bs.is_cancelled IS FALSE
    AND bwl.created_at >= (CURRENT_DATE - 1)
    AND bwl.created_at < (CURRENT_DATE)
    ORDER BY recorded_at