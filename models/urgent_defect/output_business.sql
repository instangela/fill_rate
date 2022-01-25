{{ config(
    materialized = "table",
    post_hook = "{{ unload_model_feature_to_s3() }}"
) }}

-- urgent_defect__output_business
-- key, date, binary_features, int_features, float_features, categorical_features
