{{ config(
    materialized = "table",
    post_hook = "{{ unload_model_feature_to_s3('urgent_defect') }}"
) }}


SELECT
    (CURRENT_DATE - 1) AS ds
    , user_id AS ID_worker_id
    , AVG(CASE WHEN has_passed=1 THEN score ELSE NULL END) AS RV_FLOAT_avg_pass_quiz_score
    , COUNT(DISTINCT quiz_config_id) AS RV_INT_num_unique_quizes
    , COUNT(has_passed) AS RV_INT_total_quiz_attempts
    , SUM(has_passed) AS RV_INT_total_quizes_passed
    , DATEDIFF(DAY, MIN(created_at), CURRENT_DATE) AS RV_INT_num_days_from_earliest_attempt
    , DATEDIFF(DAY, MAX(created_at), CURRENT_DATE) AS RV_INT_num_days_from_latest_attempt
    , MIN(created_at) AS TS_earliest_quiz_attempt
    , MAX(created_at) AS TS_latest_quiz_attempt
FROM
    {{ source("instawork-dw-backend", "backend_workerquizscore") }}
WHERE 
    quiz_config_id IS NOT NULL
    AND created_at < CURRENT_DATE
GROUP BY ID_worker_id
ORDER BY ID_worker_id