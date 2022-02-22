SELECT
    CURRENT_DATE - 1 AS ds
    , bzs.business_id AS {{ id_feature('business_id') }}
    , bzs.company_id  AS {{ id_feature('company_id') }}
    , biz_rm.displayname AS {{ categorical_string_feature('business_region_name') }}
    , bzs.business_type AS {{ categorical_string_feature('business_type') }}
    , bzs.{{ integer_feature('total_shifts') }} AS {{ integer_feature('total_shifts') }}
    , bzs.{{ integer_feature('total_filled_shifts') }} AS {{ integer_feature('total_filled_shifts') }}
    , bzs.{{ integer_feature('w2_employees_only') }} AS {{ integer_feature('w2_employees_only') }}
    , bzs.{{ float_feature('avg_business_rating_by_worker') }} AS {{ float_feature('avg_business_rating_by_worker') }}
    , bzs.{{ integer_feature('total_ratings_by_workers') }} AS {{ integer_feature('total_ratings_by_workers') }}
    , bzs.{{ float_feature('avg_worker_rating_by_business') }} AS {{ float_feature('avg_worker_rating_by_business') }}
    , bzs.{{ integer_feature('total_ratings_by_business') }} AS {{ integer_feature('total_ratings_by_business') }}
    , bzs.{{ float_feature('avg_filled_shift_business_rate') }} AS {{ float_feature('avg_filled_shift_business_rate') }}
    , bzs.{{ float_feature('avg_unfilled_shift_business_rate') }} AS {{ float_feature('avg_unfilled_shift_business_rate') }}
    , bzs.{{ float_feature('avg_filled_shift_applicant_rate') }} AS {{ float_feature('avg_filled_shift_applicant_rate') }}
    , bzs.{{ float_feature('avg_unfilled_shift_applicant_rate') }} AS {{ float_feature('avg_unfilled_shift_applicant_rate') }}
    , bcs.{{ integer_feature('total_cancelled_shifts') }} AS {{ integer_feature('total_cancelled_shifts') }}
    , bcs.{{ integer_feature('cancelled_filled_shifts') }} AS {{ integer_feature('cancelled_filled_shifts') }}
    , pp.timezone AS {{ categorical_string_feature('business_timezone') }}
    -- use correct company timezones
    , CASE WHEN DATE_TRUNC('WEEK', csh.first_gig_date) = DATE_TRUNC('WEEK', convert_timezone('PDT', CURRENT_DATE)) then 'New Partner' else 'Existing Partner' end as {{ categorical_string_feature('partner_status') }}
    , CASE WHEN DATEDIFF('DAY', csh.first_gig_date, DATE(convert_timezone('PDT', CURRENT_DATE))) <= 30 then 'F30' else 'Existing' end as {{ categorical_string_feature('partner_period') }}
    , CASE WHEN hc.company_id is NOT NULL THEN 'Hospitality' ELSE 'Light Industrial' END AS {{ categorical_string_feature('partner_type') }}
FROM (
    SELECT
        gt.business_id
        , gt.company_id
        , bt.display_name as business_type
        , COUNT(*) AS {{ integer_feature('total_shifts') }}
        , SUM(CASE WHEN bs.worker_id IS NOT NULL THEN 1 ELSE 0 END) AS {{ integer_feature('total_filled_shifts') }}
        , SUM(gt.w2_employees_only) AS {{ integer_feature('w2_employees_only') }}
        , AVG(bs.rating_by_worker) AS {{ float_feature('avg_business_rating_by_worker') }}
        , SUM(CASE WHEN bs.worker_id IS NOT NULL AND bs.rating_by_worker IS NOT NULL THEN 1 ELSE 0 END) AS {{ integer_feature('total_ratings_by_workers') }}
        , AVG(bs.rating_by_business) AS {{ float_feature('avg_worker_rating_by_business') }}
        , SUM(CASE WHEN bs.worker_id IS NOT NULL AND bs.rating_by_business IS NOT NULL THEN 1 ELSE 0 END) AS {{ integer_feature('total_ratings_by_business') }}
        , AVG(CASE WHEN bs.worker_id IS NOT NULL THEN bs.business_rate_usd ELSE NULL END) AS {{ float_feature('avg_filled_shift_business_rate') }}
        , AVG(CASE WHEN bs.worker_id IS NULL THEN bs.business_rate_usd ELSE NULL END) AS {{ float_feature('avg_unfilled_shift_business_rate') }}
        , AVG(CASE WHEN bs.worker_id IS NOT NULL THEN bs.applicant_rate_usd ELSE NULL END) AS {{ float_feature('avg_filled_shift_applicant_rate') }}
        , AVG(CASE WHEN bs.worker_id IS NULL THEN bs.applicant_rate_usd ELSE NULL END) AS {{ float_feature('avg_unfilled_shift_applicant_rate') }}
    FROM
        iw_backend_db.backend_shift bs
        LEFT JOIN iw_backend_db.backend_shiftgroup sg ON sg.id = bs.shift_group_id
        LEFT JOIN iw_backend_db.backend_gigtemplate gt ON gt.id = bs.gig_id
        LEFT JOIN iw_backend_db.backend_company c ON c.id = gt.company_id
        LEFT JOIN iw_backend_db.backend_businesstype bt on bt.id = c.business_type_reference
        LEFT JOIN (
        SELECT 
            distinct(overbook_shiftgroup_id) AS overbook_shiftgroup_id
        FROM 
            iw_backend_db.backend_shiftgroup
        ) ob ON ob.overbook_shiftgroup_id = sg.id
    WHERE
        1 = 1
        AND bs.is_cancelled IS FALSE
        AND sg.starts_at < CURRENT_DATE
        AND ob.overbook_shiftgroup_id IS NULL
        AND gt.position_fk_id <> 25
        AND gt.is_draft IS FALSE
        AND c.is_internal_only = 0
        AND gt.business_id IS NOT NULL  
    GROUP BY 
        1, 2, 3
) bzs
LEFT JOIN iw_backend_db.business biz ON biz.id = bzs.business_id
LEFT JOIN iw_backend_db.places_place pp ON pp.id = biz.place_id
LEFT JOIN iw_backend_db.backend_regionmapping biz_rm ON biz_rm.id = pp.regionmapping_id
LEFT JOIN (
    SELECT
        gt.business_id
        , COUNT(*) AS {{ integer_feature('total_cancelled_shifts') }}
        , SUM(CASE WHEN worker_id IS NOT NULL THEN 1 ELSE 0 END) AS {{ integer_feature('cancelled_filled_shifts') }}
    FROM
        iw_backend_db.backend_shift bs
        LEFT JOIN iw_backend_db.backend_shiftgroup sg ON sg.id = bs.shift_group_id
        LEFT JOIN iw_backend_db.backend_gigtemplate gt ON gt.id = bs.gig_id
        LEFT JOIN iw_backend_db.backend_company c ON c.id = gt.company_id
        LEFT JOIN (
        SELECT 
            distinct(overbook_shiftgroup_id) AS overbook_shiftgroup_id
        FROM 
            iw_backend_db.backend_shiftgroup
        ) ob ON ob.overbook_shiftgroup_id = sg.id
    WHERE
        bs.is_cancelled IS TRUE
        AND sg.starts_at < CURRENT_DATE
        AND gt.position_fk_id <> 25
        AND ob.overbook_shiftgroup_id IS NULL
        AND gt.is_draft IS FALSE
        AND c.is_internal_only = 0    
    GROUP BY 
        1
) bcs ON bcs.business_id = bzs.business_id
LEFT JOIN (
    SELECT
        gt.company_id
        -- , gt.business_id
        , DATE(MIN(CASE WHEN bs.worker_id IS NOT NULL THEN convert_timezone('PDT', sg.starts_at) ELSE NULL END)) AS first_gig_date
        , DATE(MIN(convert_timezone('PDT', sg.created_at))) AS first_booking_date
    FROM
        iw_backend_db.backend_shift bs
        LEFT JOIN iw_backend_db.backend_shiftgroup sg ON sg.id = bs.shift_group_id
        LEFT JOIN iw_backend_db.backend_gigtemplate gt ON gt.id = bs.gig_id
        LEFT JOIN iw_backend_db.business business ON gt.business_id = business.id
        LEFT JOIN iw_backend_db.backend_company c ON c.id = gt.company_id
        LEFT JOIN (
        SELECT 
            distinct(overbook_shiftgroup_id) AS overbook_shiftgroup_id
        FROM 
            iw_backend_db.backend_shiftgroup
        ) ob ON ob.overbook_shiftgroup_id = sg.id
    WHERE
        1 = 1
        AND bs.is_cancelled is FALSE
        AND ob.overbook_shiftgroup_id IS NULL
        AND gt.position_fk_id <> 25
        AND gt.is_draft IS FALSE
        AND c.is_internal_only = 0
    GROUP BY 1 --, 2
) csh ON bzs.company_id = csh.company_id
LEFT JOIN ( 
    SELECT 
        DISTINCT gt.company_id
    FROM
        iw_backend_db.backend_shift bs
        LEFT JOIN iw_backend_db.backend_shiftgroup sg ON sg.id = bs.shift_group_id
        LEFT JOIN iw_backend_db.backend_gigtemplate gt ON gt.id = bs.gig_id
        LEFT JOIN iw_backend_db.business business ON gt.business_id = business.id
        JOIN iw_backend_db.backend_company c on c.id = gt.company_id
        LEFT JOIN (
        SELECT 
            distinct(overbook_shiftgroup_id) AS overbook_shiftgroup_id
        FROM 
            iw_backend_db.backend_shiftgroup
        ) ob ON ob.overbook_shiftgroup_id = sg.id
        JOIN iw_backend_db.backend_businesstype bt on bt.id = c.business_type_reference
    WHERE 
        1=1
        AND bs.is_cancelled IS FALSE
        AND bs.worker_id IS NOT NULL
        AND gt.position_fk_id <> 25
        AND gt.is_draft IS FALSE
        AND c.is_internal_only = 0
        AND gt.position_fk_id NOT IN (29, 30, 31, 37, 39, 42, 46)
        AND gt.company_id NOT IN  (2848, 8428, 16207, 13360, 19951, 20541, 19737, 19696, 14368, 21398, 21341, 18694, 20744, 15731, 21921, 21341, 19643)
        AND bt.id != 5 
        AND sg.created_at < CURRENT_DATE
        AND ob.overbook_shiftgroup_id IS NULL
) hc ON hc.company_id = bzs.company_id
