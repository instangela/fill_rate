SELECT 
    CURRENT_DATE AS ds
    , ID_worker_id
    -- count sessions
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_1_day' THEN 1 ELSE 0 END) AS {{ integer_feature('lte_1_day_num_sessions') }}
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_3_days' THEN 1 ELSE 0 END) AS RV_INT_lte_3_day_num_sessions
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_7_days' THEN 1 ELSE 0 END) AS RV_INT_lte_7_day_num_sessions
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_30_days' THEN 1 ELSE 0 END) AS RV_INT_lte_30_day_num_sessions
    , SUM(CASE WHEN MC_event_occurence_range = 'gt_30_days' THEN 1 ELSE 0 END) AS RV_INT_gt_30_day_num_sessions
    -- session time spent
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_1_day' THEN RV_INT_session_length_seconds ELSE 0 END) AS RV_INT_lte_1_day_session_length_seconds
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_3_days' THEN RV_INT_session_length_seconds ELSE 0 END) AS RV_INT_lte_3_day_session_length_seconds
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_7_days' THEN RV_INT_session_length_seconds ELSE 0 END) AS RV_INT_lte_7_day_session_length_seconds
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_30_days' THEN RV_INT_session_length_seconds ELSE 0 END) AS RV_INT_lte_30_day_session_length_seconds
    , SUM(CASE WHEN MC_event_occurence_range = 'gt_30_days' THEN RV_INT_session_length_seconds ELSE 0 END) AS RV_INT_gt_30_day_session_length_seconds
    -- event type counters
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_1_day' THEN RV_INT_num_event_types ELSE 0 END) AS RV_INT_lte_1_day_num_event_types
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_3_days' THEN RV_INT_num_event_types ELSE 0 END) AS RV_INT_lte_3_day_num_event_types
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_7_days' THEN RV_INT_num_event_types ELSE 0 END) AS RV_INT_lte_7_day_num_event_types
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_30_days' THEN RV_INT_num_event_types ELSE 0 END) AS RV_INT_lte_30_day_num_event_types
    , SUM(CASE WHEN MC_event_occurence_range = 'gt_30_days' THEN RV_INT_num_event_types ELSE 0 END) AS RV_INT_gt_30_day_num_event_types
    -- unique event types
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_1_day' THEN RV_INT_num_unique_event_types ELSE 0 END) AS RV_INT_lte_1_day_num_unique_event_types
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_3_days' THEN RV_INT_num_unique_event_types ELSE 0 END) AS RV_INT_lte_3_day_num_unique_event_types
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_7_days' THEN RV_INT_num_unique_event_types ELSE 0 END) AS RV_INT_lte_7_day_num_unique_event_types
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_30_days' THEN RV_INT_num_unique_event_types ELSE 0 END) AS RV_INT_lte_30_day_num_unique_event_types
    , SUM(CASE WHEN MC_event_occurence_range = 'gt_30_days' THEN RV_INT_num_unique_event_types ELSE 0 END) AS RV_INT_gt_30_day_num_unique_event_types
    -- unique event ids
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_1_day' THEN RV_INT_num_unique_event_id ELSE 0 END) AS RV_INT_lte_1_day_num_unique_event_id
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_3_days' THEN RV_INT_num_unique_event_id ELSE 0 END) AS RV_INT_lte_3_day_num_unique_event_id
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_7_days' THEN RV_INT_num_unique_event_id ELSE 0 END) AS RV_INT_lte_7_day_num_unique_event_id
    , SUM(CASE WHEN MC_event_occurence_range = 'lte_30_days' THEN RV_INT_num_unique_event_id ELSE 0 END) AS RV_INT_lte_30_day_num_unique_event_id
    , SUM(CASE WHEN MC_event_occurence_range = 'gt_30_days' THEN RV_INT_num_unique_event_id ELSE 0 END) AS RV_INT_gt_30_day_num_unique_event_id
FROM (
    SELECT
        DATE_TRUNC('DAY', client_event_time) AS TS_event_day
        , CAST(user_id AS INTEGER) AS ID_worker_id
        , session_id AS ID_session_id
        , DATEDIFF('DAY' , MIN(client_event_time), CURRENT_DATE) AS RV_INT_num_days_ago
        , CASE 
            WHEN DATEDIFF('DAY' , MIN(client_event_time), CURRENT_DATE) <= 1 THEN 'lte_1_day'
            WHEN DATEDIFF('DAY' , MIN(client_event_time), CURRENT_DATE) <= 3 THEN 'lte_3_days'
            WHEN DATEDIFF('DAY' , MIN(client_event_time), CURRENT_DATE) <= 7 THEN 'lte_7_days'
            WHEN DATEDIFF('DAY' , MIN(client_event_time), CURRENT_DATE) <= 30 THEN 'lte_30_days'
            ELSE 'gt_30_days'
        END AS MC_event_occurence_range
        , MIN(client_event_time) AS TS_session_start_time
        , MAX(client_event_time) AS TS_session_end_time
        , DATEDIFF('SECOND' , MIN(client_event_time), MAX(client_event_time)) AS RV_INT_session_length_seconds
        , COUNT(event_type) AS RV_INT_num_event_types
        , COUNT(event_id) AS RV_INT_num_event_ids
        , COUNT(DISTINCT event_type) AS RV_INT_num_unique_event_types
        , COUNT(DISTINCT event_id) AS RV_INT_num_unique_event_id
    FROM {{ source("instawork-dw-amplitude", "schema_173137-events_173137") }}
    WHERE
        client_event_time > CURRENT_DATE - 31
        AND client_event_time < CURRENT_DATE
        AND ID_session_id > 0
        AND user_id IS NOT NULL
    GROUP BY 1, 2, 3
    ORDER BY 1, 2
)
GROUP BY ID_worker_id
