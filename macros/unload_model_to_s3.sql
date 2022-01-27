{% macro unload_model_feature_to_s3(model_name) %}

{% set iam_role %}arn:aws:iam::183605072238:role/RedshiftMLTransformsRole{% endset %}

-- line up dev + prod prefix from lambda function
{% set stage %}{% if this.target == "staging" %}dev{% else %}prod{% endif %}{% endset %}
{% set s3_path %}s3://instawork-ml/transforms/{{ this.schema }}/{{ this.table }}__{% endset %}

{% set sql %}
{{ redshift.unload_table(this.schema, this.table, iam_role=iam_role, s3_path=s3_path, header=True, delimiter='|', parallel=False, max_file_size='5 mb', overwrite=True) }}
{% endset %}

-- only unload for staging and production
{% if this.target == "staging" or this.target == "production" %}
    {% do log("Unloading feature " ~ this.table ~ " to " ~ s3_path ~ "*", info=True) %}
    {{ sql }}
{% else %}
    {% do log("Skipping unloading of feature " ~ this.table ~ " for " ~ this.schema, info=True) %} 
{% endif %}

{% endmacro %}
