-- override schema name when target isn't `dev` so schema is ml-staging, ml-prod, etc
{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- if target.name == 'dev' -%}
        {{ target.schema }}
    {%- else -%}
        ml-{{ target.name }}
    {%- endif -%}
{%- endmacro %}

-- override name of model (table) to prefix with ML model name
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {% set package_name = node.package_name %}
    {%- if custom_alias_name is none -%}
        {%- if package_name == 'ml_transforms' -%}
            {% set schema_file_path = node.original_file_path %} 
            {% set split_name = schema_file_path.split('/') %}
            {%- if split_name|length > 2 -%}
                {% set table_prefix = split_name[1] %}
                {{ table_prefix | trim }}_{{ node.name }}
            {%- else -%}
                {{ node.name }}
            {%- endif -%}
        {%- else -%}
            {{ node.name }}
        {%- endif -%}
    {%- else -%}
        {{ custom_alias_name | trim }}
    {%- endif -%}
{%- endmacro %}