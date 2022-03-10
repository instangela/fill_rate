{% macro unload_model_feature_to_s3(filesystem="append") %}

{# used to validate our filesystem argument #}
{% set filesystems = ["append", "drop"] %}
{% if filesystem not in filesystems %}
    {{ exceptions.raise_compiler_error("Invalid `filesystem`. Got: '" ~ filesystem ~ "'. Expected one of " ~ filesystems) }}
{% endif %}

{% set iam_role %}arn:aws:iam::183605072238:role/RedshiftMLTransformsRole{% endset %}

{# line up dev + prod prefix from lambda function #}
{% set stage %}{% if target.name == "production" %}prod{% else %}dev{% endif %}{% endset %}
{% set s3_path %}s3://instawork-ml-{{ stage }}/transforms/{{ stage }}/{{ invocation_id }}/{{ this.table }}__{{ filesystem }}__{% endset %}

{% set sql %}
{{ unload_table(
    this.schema,
    this.table,
    iam_role=iam_role,
    s3_path=s3_path,
    header=True,
    delimiter='|',
    parallel=False,
    max_file_size='5 mb',
    overwrite=True,
    where="invocation_uuid = '" ~ invocation_id ~ "'"
  )
}}
{% endset %}

{# only unload for staging and production #}
{% if target.type == "redshift" %}
    {% if target.name == "staging" or target.name == "production" %}
        {% do log("Unloading feature " ~ this.table ~ " to " ~ s3_path ~ "*", info=True) %}
        {{ sql }}
    {% else %}
        {% do log("Skipping unloading of feature " ~ this.table ~ " for " ~ this.schema, info=True) %}
        SELECT 1
    {% endif %}
{% else %}
    {% do log("UNLOAD is only supported by Redshift. Skipping unloading of feature " ~ this.table ~ " for " ~ this.schema, info=True) %}
    SELECT 1 
{% endif %}

{% endmacro %}
