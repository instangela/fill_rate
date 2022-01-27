{{ config(
    materialized = "table",
    post_hook = "{{ unload_model_feature_to_s3() }}"
) }}

-- note: this table will be copied to ml-instawork-* as the following via S3
-- urgent_defect__business
-- key, date, meta_features, binary_features, int_features, float_features, timestamp_features, categorical_features

SELECT
    (pbff.ID_worker_id || '_' || pbff.ID_business_id || '_' || pbff.MC_business_region) AS key,
    CURRENT_DATE AS date,
    {{ dbt_utils.star(from=ref('pro_business_future_features'), except=["ID_worker_id", "ID_business_id", "MC_business_region", "ds"]) }},
    {{ dbt_utils.star(from=ref('pro_business_history_features'), except=["ID_worker_id", "ID_business_id", "MC_business_region", "ds"]) }}
FROM {{ ref('pro_business_future_features') }} pbff
LEFT JOIN {{ ref('pro_business_history_features') }} USING (ID_worker_id, ID_business_id, MC_business_region)