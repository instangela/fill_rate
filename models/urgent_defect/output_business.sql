{{ config(
    materialized = "table",
    post_hook = "{{ unload_model_feature_to_s3() }}"
) }}

-- note: this table will be copied to ml-instawork-* as the following via S3
-- urgent_defect__business
-- key, date, meta_features, binary_features, int_features, float_features, timestamp_features, categorical_features

SELECT
    bf.ID_business_id AS key,
    CURRENT_DATE AS date,
    {{ dbt_utils.star(from=ref('business_features'), except=["ID_business_id", "ds"]) }}
FROM {{ ref('business_features') }} bf
