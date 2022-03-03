{{ config(
    materialized = "incremental",
    post_hook = "{{ unload_model_feature_to_s3('drop') }}",
    on_schema_change = "append_new_columns"
) }}

-- note: this table will be copied to ml-instawork-* as the following via S3
-- urgent_defect__business
-- key, date, meta_features, binary_features, int_features, float_features, timestamp_features, categorical_features

-- Not using is_incremental() macro as we dont want to create a only incremental data
-- the feature vector on a daily basis should be self contained and not an increment over previous day

SELECT
    bf.ID_business_id AS key,
    (CURRENT_DATE - 1) AS date,
    '{{ invocation_id }}' AS invocation_uuid,
    {{ dbt_utils.star(from=ref('business_features'), except=["ID_business_id", "ds"]) }}
FROM {{ ref('business_features') }} bf
