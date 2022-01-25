{% macro unload_model_feature_to_s3(model_name) %}

{% set iam_role %}arn:aws:iam::183605072238:role/RedshiftMLTransformsRole{% endset %}
{% set s3_path %}s3://instawork-ml/transforms/{{ this.schema }}/{{ this.table }}__{% endset %}

{% set sql %}
{{ redshift.unload_table(this.schema, this.table, iam_role=iam_role, s3_path=s3_path, header=True, delimiter='|', parallel=False, max_file_size='5 mb', overwrite=True) }}
{% endset %}

{% do log("Unloading feature " ~ this.table ~ " to " ~ s3_path ~ "*", info=True) %}
{{ sql }}

{% endmacro %}
