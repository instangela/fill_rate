{{ config(
    materialized = "incremental",
    post_hook = "{{ unload_model_feature_to_s3('drop') }}",
    on_schema_change = "append_new_columns"
) }}

-- note: this table will be copied to ml-instawork-* as the following via S3
-- urgent_defect__pro
-- key, date, meta_features, binary_features, int_features, float_features, timestamp_features, categorical_features

-- Not using is_incremental() macro as we dont want to create a only incremental data
-- the feature vector on a daily basis should be self contained and not an increment over previous day

SELECT
    ppf.ID_worker_id AS key,
    (CURRENT_DATE - 1) AS date,
    '{{ invocation_id }}' AS invocation_uuid,
    {{ dbt_utils.star(from=ref('pro_amplitude_session_features'), except=["ID_worker_id", "ds"]) }},
    {{ dbt_utils.star(from=ref('pro_future_features'), except=["ID_worker_id", "ds"]) }}, 
    {{ dbt_utils.star(from=ref('pro_history_features'), except=["ID_worker_id", "ds"]) }}, 
    {{ dbt_utils.star(from=ref('pro_profile_features'), except=["ID_worker_id", "ds"]) }}, 
    {{ dbt_utils.star(from=ref('pro_quiz_aggregate_features'), except=["ID_worker_id", "ds"]) }}, 
    {{ dbt_utils.star(from=ref('pro_worker_experience_features'), except=["ID_worker_id", "ds"]) }}
FROM {{ ref('pro_profile_features') }} ppf
LEFT JOIN {{ ref('pro_future_features') }} USING (ID_worker_id)
LEFT JOIN {{ ref('pro_history_features') }} USING (ID_worker_id)
LEFT JOIN {{ ref('pro_amplitude_session_features') }} pasf USING (ID_worker_id)
LEFT JOIN {{ ref('pro_quiz_aggregate_features') }} USING (ID_worker_id)
LEFT JOIN {{ ref('pro_worker_experience_features') }} USING (ID_worker_id)
WHERE ppf.{{ categorical_string_feature('worker_status') }} = 'active'