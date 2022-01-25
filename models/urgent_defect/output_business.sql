{{ config(
    materialized = "table",
    post_hook = "{{ unload_model_feature_to_s3() }}"
) }}

-- note: this table will be copied to ml-instawork-* as the following via S3
-- urgent_defect__business
-- key, date, binary_features, int_features, float_features, timestamp_features, categorical_features
