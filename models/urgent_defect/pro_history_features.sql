SELECT 
    ds
    , ID_worker_id
    , COALESCE(RV_INT_running_worker_assigneds, 0) AS RV_INT_running_worker_assigneds
    , COALESCE(RV_INT_running_worker_unassigneds, 0) AS RV_INT_running_worker_unassigneds
    , COALESCE(RV_INT_running_business_cancels, 0) AS RV_INT_running_business_cancels
    , COALESCE(RV_INT_running_no_shows, 0) AS RV_INT_running_no_shows
    , COALESCE(RV_INT_running_no_shows_corrected, 0) AS RV_INT_running_no_shows_corrected
    , COALESCE(RV_INT_running_auto_cancels, 0) AS RV_INT_running_auto_cancels
    , COALESCE(RV_INT_running_worker_cancels, 0) AS RV_INT_running_worker_cancels
    , COALESCE(RV_INT_running_excuse_cancels, 0) AS RV_INT_running_excuse_cancels
    , COALESCE(RV_INT_running_minor_tardies, 0) AS RV_INT_running_minor_tardies
    , COALESCE(RV_INT_running_major_tardies, 0) AS RV_INT_running_major_tardies
    , COALESCE(RV_INT_running_shift_leads, 0) AS RV_INT_running_shift_leads  
FROM (
    SELECT 
        CURRENT_DATE - 1 AS ds
        , ID_worker_id
        , ROW_NUMBER() OVER (PARTITION BY ID_worker_id ORDER BY starts_at DESC) AS rn_worker
        , ROW_NUMBER() OVER (PARTITION BY ID_worker_id, ID_business_id ORDER BY starts_at DESC) AS rn_worker_business
        , COALESCE(RV_INT_running_worker_assigneds, 0) AS RV_INT_running_worker_assigneds
        , COALESCE(RV_INT_running_worker_unassigneds, 0) AS RV_INT_running_worker_unassigneds
        , COALESCE(RV_INT_running_business_cancels, 0) AS RV_INT_running_business_cancels
        , COALESCE(RV_INT_running_no_shows, 0) AS RV_INT_running_no_shows
        , COALESCE(RV_INT_running_no_shows_corrected, 0) AS RV_INT_running_no_shows_corrected
        , COALESCE(RV_INT_running_auto_cancels, 0) AS RV_INT_running_auto_cancels
        , COALESCE(RV_INT_running_worker_cancels, 0) AS RV_INT_running_worker_cancels
        , COALESCE(RV_INT_running_excuse_cancels, 0) AS RV_INT_running_excuse_cancels
        , COALESCE(RV_INT_running_minor_tardies, 0) AS RV_INT_running_minor_tardies
        , COALESCE(RV_INT_running_major_tardies, 0) AS RV_INT_running_major_tardies
        , COALESCE(RV_INT_running_shift_leads, 0) AS RV_INT_running_shift_leads
    FROM (
        SELECT
            psi.ID_worker_id
            , business.id AS ID_business_id
            , psi.ID_shift_id
            , sg.starts_at
            , SUM(psi.B_is_worker_assigned) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_worker_assigneds
            , SUM(psi.B_is_worker_unassigned) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_worker_unassigneds
            , SUM(psi.B_is_business_cancel) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_business_cancels
            , SUM(psi.B_is_no_show) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_no_shows
            , SUM(psi.B_is_no_show_corrected) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_no_shows_corrected
            , SUM(psi.B_is_auto_cancel) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_auto_cancels
            , SUM(psi.B_is_worker_cancel) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_worker_cancels
            , SUM(psi.B_is_excuse_cancel) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_excuse_cancels
            , SUM(psi.B_is_minor_tardy) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_minor_tardies
            , SUM(psi.B_is_major_tardy) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_major_tardies
            , SUM(psi.B_is_shift_lead) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_shift_leads
            , SUM(psi.B_is_worker_assigned) OVER (PARTITION BY psi.ID_worker_id, business.id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)  AS RV_INT_running_assigneds_with_business
            , SUM(psi.B_is_worker_unassigned) OVER (PARTITION BY psi.ID_worker_id, business.id ORDER BY sg.starts_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS RV_INT_running_unassigneds_with_business
            -- Future Counter Features
            , SUM(psi.B_is_worker_assigned) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS RV_INT_future_running_worker_assigneds
            , SUM(psi.B_is_worker_unassigned) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS RV_INT_future_running_worker_unassigneds
            , SUM(psi.B_is_business_cancel) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS RV_INT_future_running_business_cancels
            , SUM(psi.B_is_no_show) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS RV_INT_future_running_no_shows
            , SUM(psi.B_is_no_show_corrected) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS RV_INT_future_running_no_shows_corrected
            , SUM(psi.B_is_auto_cancel) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS RV_INT_future_running_auto_cancels
            , SUM(psi.B_is_worker_cancel) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS RV_INT_future_running_worker_cancels
            , SUM(psi.B_is_excuse_cancel) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS RV_INT_future_running_excuse_cancels
            , SUM(psi.B_is_shift_lead) OVER (PARTITION BY psi.ID_worker_id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS RV_INT_future_running_shift_leads
            , SUM(psi.B_is_worker_assigned) OVER (PARTITION BY psi.ID_worker_id, business.id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)  AS RV_INT_FUTURE_running_assigneds_with_business
            , SUM(psi.B_is_worker_unassigned) OVER (PARTITION BY psi.ID_worker_id, business.id ORDER BY sg.starts_at ASC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS RV_INT_FUTURE_running_unassigneds_with_business
        FROM
        (
            SELECT
                worker_id AS ID_worker_id,
                shift_id AS ID_shift_id,
                MAX(CASE WHEN event = 52 THEN 1 ELSE 0 END) AS B_is_event_hold_active,
                MAX(CASE WHEN event = 51 THEN 1 ELSE 0 END) AS B_is_event_hold_released,
                MAX(CASE WHEN event = 9 THEN 1 ELSE 0 END) AS B_is_worker_assigned,
                -- should this be MIN()?, we have multiple 9's when we do no-show correction eg. shift_id 1313856
                -- also there are some manual updates which do not have any no-show correction issues eg. shift_id 450001
                MAX(CASE WHEN event = 9 THEN recorded_at ELSE NULL END) AS TS_worker_assigned_at,
                MAX(CASE WHEN event = 10 THEN 1 ELSE 0 END) AS B_is_worker_unassigned,
                MAX(CASE WHEN event = 10 THEN recorded_at ELSE NULL END) AS TS_worker_unassigned_at,
                MAX(CASE WHEN event = 1 THEN 1 ELSE 0 END) AS B_is_business_cancel,
                MAX(CASE WHEN event = 15 THEN 1 ELSE 0 END) AS B_is_no_show,
                MAX(CASE WHEN event = 15 THEN recorded_at ELSE NULL END) AS TS_no_showed_at,
                MAX(CASE WHEN event = 55 THEN 1 ELSE 0 END) AS B_is_no_show_corrected,
                MAX(CASE WHEN event = 55 THEN recorded_at ELSE NULL END) AS TS_no_show_corrected_at,
                MAX(CASE WHEN event = 26 THEN 1 ELSE 0 END) AS B_is_worker_cancel,
                MAX(CASE WHEN event = 26 THEN recorded_at ELSE NULL END) AS TS_worked_cancelled_at,
                MAX(CASE WHEN event = 56 THEN 1 ELSE 0 END) AS B_is_excuse_cancel,
                MAX(CASE WHEN event = 56 THEN recorded_at ELSE NULL END) AS TS_excuse_cancelled_at,
                MAX(CASE WHEN event = 43 THEN 1 ELSE 0 END) AS B_is_auto_cancel,
                MAX(CASE WHEN event = 43 THEN recorded_at ELSE NULL END) AS TS_auto_cancelled_at,
                MAX(CASE WHEN event = 5 THEN 1 ELSE 0 END) AS B_is_rated_by_worker,
                MAX(CASE WHEN event = 6 THEN 1 ELSE 0 END) AS B_is_rated_by_business,
                MAX(CASE WHEN event = 47 THEN 1 ELSE 0 END) AS B_is_minor_tardy,
                MAX(CASE WHEN event = 48 THEN 1 ELSE 0 END) AS B_is_major_tardy,
                MAX(CASE WHEN event = 66 THEN 1 ELSE 0 END) AS B_is_shift_lead,
                MIN(CASE WHEN event = 66 THEN recorded_at ELSE NULL END) AS TS_shift_lead_assigned_at
            FROM {{ source("instawork-dw-backend", "backend_workerlog") }}
            WHERE 
                shift_id IS NOT NULL
                AND is_deleted IS FALSE
            GROUP BY
                worker_id,
                shift_id
            ORDER BY shift_id DESC
        ) psi
        LEFT JOIN {{ source("instawork-dw-backend", "backend_shift") }} bs ON bs.id = psi.ID_shift_id
        LEFT JOIN {{ source("instawork-dw-backend", "backend_shiftgroup") }} sg ON sg.id = bs.shift_group_id
        LEFT JOIN {{ source("instawork-dw-backend", "backend_gigtemplate") }} gt ON gt.id = bs.gig_id
        LEFT JOIN {{ source("instawork-dw-backend", "business") }} ON gt.business_id = business.id
        -- LEFT JOIN {{ source("instawork-dw-backend", "positions_position") }} gig_position ON gig_position.id = gt.position_fk_id
        WHERE
            sg.starts_at < SYSDATE
        ORDER BY ID_worker_id, sg.starts_at DESC -- order in chronologically increasing order
    )
ORDER BY ID_worker_id, rn_worker
)
WHERE rn_worker = 1
