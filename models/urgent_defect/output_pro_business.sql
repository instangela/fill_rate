{{ config(
    materialized = "incremental",
    post_hook = "{{ unload_model_feature_to_s3('drop') }}",
    on_schema_change = "append_new_columns"
) }}

-- note: this table will be copied to ml-instawork-* as the following via S3
-- urgent_defect__pro_business
-- key, date, meta_features, binary_features, int_features, float_features, timestamp_features, categorical_features

-- Not using is_incremental() macro as we dont want to create a only incremental data
-- the feature vector on a daily basis should be self contained and not an increment over previous day

SELECT
    (pbff.ID_worker_id || '_' || pbff.ID_business_id) AS key,
    (CURRENT_DATE - 1) AS date,
    '{{ invocation_id }}' AS invocation_uuid,
    {{ dbt_utils.star(from=ref('pro_business_future_features'), except=["ID_worker_id", "ID_business_id", "MC_business_region", "ds"]) }},
    {{ dbt_utils.star(from=ref('pro_business_history_features'), except=["ID_worker_id", "ID_business_id", "MC_business_region", "ds"]) }}
FROM {{ ref('pro_business_future_features') }} pbff
LEFT JOIN {{ ref('pro_business_history_features') }} USING (ID_worker_id, ID_business_id, MC_business_region)