-- macros used to name features for S3 to MySQL conversion

{% macro binary_feature(column) %}B_{{ column }}{% endmacro %}
{% macro categorical_integer_feature(column) %}MC_INT_{{ column }}{% endmacro %}
{% macro categorical_string_feature(column) %}MC_STR_{{ column }}{% endmacro %}
{% macro integer_feature(column) %}RV_INT_{{ column }}{% endmacro %}
{% macro float_feature(column) %}RV_FLOAT_{{ column }}{% endmacro %}
{% macro meta_feature(column) %}MT_{{ column }}{% endmacro %}
{% macro timestamp_feature(column) %}TS_{{ column }}{% endmacro %}