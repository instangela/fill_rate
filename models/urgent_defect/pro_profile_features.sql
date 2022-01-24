{{ config(
    materialized = "table",
    post_hook = "{{ unload_model_feature_to_s3('urgent_defect') }}"
) }}

SELECT
    ds
    , wpi.ID_worker_id
    , B_is_email_verified
    , B_is_phonenum_verified
    , B_is_unsubscribe
    , B_has_resume
    , B_is_device
    , B_is_partial
    , B_is_unsubscribe_introductions
    , B_is_unsubscribe_looking
    , B_is_reference_signup
    , B_can_carpool
    , B_has_vehicle
    , B_has_driving_license
    , B_is_video_approved
    , B_has_limited_gig_access
    , B_has_followed_region_onboarding
    , B_has_work_experience
    , B_motor_vehicle_check_status
    , B_w2_eligible
    , MC_background_check_status
    , MC_w2_status
    , MC_applicant_app_os
    , RV_INT_days_from_last_active
    , RV_INT_days_from_last_login
    , RV_INT_days_from_last_modified
    , RV_INT_like_count
    , RV_INT_noshow_count
    , RV_INT_unlike_count
    , COALESCE(RV_INT_count_gold_level, 0) AS RV_INT_count_gold_level
    , COALESCE(RV_INT_count_silver_level, 0) AS RV_INT_count_silver_level
    , COALESCE(RV_INT_count_bronze_level, 0) AS RV_INT_count_bronze_level
    , COALESCE(RV_INT_count_platinum_level, 0) AS RV_INT_count_platinum_level
    , TS_last_active
    , TS_last_login
    , TS_latest_date_gold_achieved
    , TS_oldest_date_gold_achieved
    , TS_latest_date_silver_achieved
    , TS_oldest_date_silver_achieved
    , TS_latest_date_bronze_achieved
    , TS_oldest_date_bronze_achieved
    , TS_latest_date_platinum_achieved
    , TS_oldest_date_platinum_achieved
FROM (
    SELECT
        CURRENT_DATE - 1 AS ds,
        id AS ID_worker_id,
        -- Replace all binary column NULL values with 0
        CASE WHEN is_email_verified IS NULL THEN 0 ELSE is_email_verified END AS B_is_email_verified,
        CASE WHEN is_phonenum_verified IS NULL THEN 0 ELSE is_phonenum_verified END AS B_is_phonenum_verified,
        CASE WHEN is_unsubscribe IS NULL THEN 0 ELSE is_unsubscribe END AS B_is_unsubscribe,
        CASE WHEN has_resume IS NULL THEN 0 ELSE has_resume END AS B_has_resume,
        CASE WHEN is_device IS NULL THEN 0 ELSE is_device END AS B_is_device,
        CASE WHEN is_partial IS NULL THEN 0 ELSE is_partial END AS B_is_partial,
        CASE WHEN is_unsubscribe_introductions IS NULL THEN 0 ELSE is_unsubscribe_introductions END AS B_is_unsubscribe_introductions,
        CASE WHEN is_unsubscribe_looking IS NULL THEN 0 ELSE is_unsubscribe_looking END AS B_is_unsubscribe_looking,
        CASE WHEN is_reference_signup IS NULL THEN 0 ELSE is_reference_signup END AS B_is_reference_signup,
        CASE WHEN can_carpool IS NULL THEN 0 ELSE can_carpool END AS B_can_carpool,
        CASE WHEN has_vehicle IS NULL THEN 0 ELSE has_vehicle END AS B_has_vehicle,
        CASE WHEN has_driving_license IS NULL THEN 0 ELSE has_driving_license END AS B_has_driving_license,
        CASE WHEN is_video_approved IS NULL THEN 0 ELSE is_video_approved END AS B_is_video_approved,
        CASE WHEN has_limited_gig_access IS NULL THEN 0 ELSE has_limited_gig_access END AS B_has_limited_gig_access,
        CASE WHEN has_followed_region_onboarding IS NULL THEN 0 ELSE has_followed_region_onboarding END AS B_has_followed_region_onboarding,
        CASE WHEN has_work_experience IS NULL THEN 0 ELSE has_work_experience END AS B_has_work_experience,
        CASE WHEN w2_eligible IS NULL THEN 0 ELSE w2_eligible END AS B_w2_eligible,
        -- Replace Multi Category NULL values with 0
        CASE WHEN background_check_status IS NULL THEN 0 ELSE background_check_status END AS MC_background_check_status,
        CASE WHEN w2_status IS NULL THEN 0 ELSE w2_status END AS MC_w2_status,
        CASE WHEN applicant_app_os IS NOT NULL THEN applicant_app_os ELSE 'UNKNOWN' END AS MC_applicant_app_os,
        last_login AS TS_last_login,
        last_active AS TS_last_active,
        date_created AS TS_date_created,
        like_count AS RV_INT_like_count,
        noshow_count AS RV_INT_noshow_count,
        unlike_count AS RV_INT_unlike_count,
        worker_level AS MC_worker_level,
        worker_level_updated_at AS TS_worker_level_updated_at,
        date_modified AS TS_date_modified,
        GREATEST(CAST(DATEDIFF(DAY, COALESCE(last_active, last_login, date_created), CURRENT_DATE) AS INTEGER), 0) AS RV_INT_days_from_last_active,
        GREATEST(CAST(DATEDIFF(DAY, COALESCE(last_login, date_created), CURRENT_DATE) AS INTEGER), 0) AS RV_INT_days_from_last_login,
        GREATEST(CAST(DATEDIFF(DAY, COALESCE(date_modified, date_created), CURRENT_DATE) AS INTEGER), 0) AS RV_INT_days_from_last_modified,
        --- dob
        motor_vehicle_check_status AS B_motor_vehicle_check_status
    FROM {{ source("instawork-dw-backend", "backend_userprofile") }}
    WHERE worker_status IS NOT NULL
) wpi
LEFT JOIN (
    SELECT
        worker_id AS ID_worker_id,
        SUM(CASE WHEN level = 'gold' THEN 1 ELSE 0 END) AS RV_INT_count_gold_level,
        MAX(CASE WHEN level = 'gold' THEN created_at ELSE NULL END) AS TS_latest_date_gold_achieved,
        MIN(CASE WHEN level = 'gold' THEN created_at ELSE NULL END) AS TS_oldest_date_gold_achieved,
        SUM(CASE WHEN level = 'silver' THEN 1 ELSE 0 END) AS RV_INT_count_silver_level,
        MAX(CASE WHEN level = 'silver' THEN created_at ELSE NULL END) AS TS_latest_date_silver_achieved,
        MIN(CASE WHEN level = 'silver' THEN created_at ELSE NULL END) AS TS_oldest_date_silver_achieved,
        SUM(CASE WHEN level = 'bronze' THEN 1 ELSE 0 END) AS RV_INT_count_bronze_level,
        MAX(CASE WHEN level = 'bronze' THEN created_at ELSE NULL END) AS TS_latest_date_bronze_achieved,
        MIN(CASE WHEN level = 'bronze' THEN created_at ELSE NULL END) AS TS_oldest_date_bronze_achieved,
        SUM(CASE WHEN level = 'platinum' THEN 1 ELSE 0 END) AS RV_INT_count_platinum_level,
        MAX(CASE WHEN level = 'platinum' THEN created_at ELSE NULL END) AS TS_latest_date_platinum_achieved,
        MIN(CASE WHEN level = 'platinum' THEN created_at ELSE NULL END) AS TS_oldest_date_platinum_achieved
    FROM
        {{ source("instawork-dw-backend", "backend_workerlevellog") }}
    GROUP BY
        worker_id
) awl
ON awl.ID_worker_id = wpi.ID_worker_id