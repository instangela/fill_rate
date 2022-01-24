-- override schema name when target isn't `dev` so schema is ml-staging, ml-prod, etc

{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
  {%- if target.name == 'dev' -%}
    {{ target.schema }}
  {%- else -%}
    ml-{{ target.name }}
  {%- endif -%}
{%- endmacro %}
