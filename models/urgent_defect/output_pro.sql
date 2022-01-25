{{ config(
    materialized = "table",
    post_hook = "{{ unload_model_feature_to_s3() }}"
) }}

-- note: this table will be copied to ml-instawork-* as the following via S3
-- urgent_defect__pro
-- key, date, binary_features, int_features, float_features, timestamp_features, categorical_features

SELECT
    pasf.ID_worker_id AS key,
    CURRENT_DATE AS date,
    {{ dbt_utils.star(from=ref('pro_amplitude_session_features'), except=["ID_worker_id", "ds"]) }},
    {{ dbt_utils.star(from=ref('pro_future_features'), except=["ID_worker_id", "ds"]) }}, 
    {{ dbt_utils.star(from=ref('pro_history_features'), except=["ID_worker_id", "ds"]) }}, 
    {{ dbt_utils.star(from=ref('pro_profile_features'), except=["ID_worker_id", "ds"]) }}, 
    {{ dbt_utils.star(from=ref('pro_quiz_aggregate_features'), except=["ID_worker_id", "ds"]) }}, 
    {{ dbt_utils.star(from=ref('pro_worker_experience_features'), except=["ID_worker_id", "ds"]) }}
FROM {{ ref('pro_amplitude_session_features') }} pasf
LEFT JOIN {{ ref('pro_future_features') }} USING (ID_worker_id)
LEFT JOIN {{ ref('pro_history_features') }} USING (ID_worker_id)
LEFT JOIN {{ ref('pro_profile_features') }} USING (ID_worker_id)
LEFT JOIN {{ ref('pro_quiz_aggregate_features') }} USING (ID_worker_id)
LEFT JOIN {{ ref('pro_worker_experience_features') }} USING (ID_worker_id)