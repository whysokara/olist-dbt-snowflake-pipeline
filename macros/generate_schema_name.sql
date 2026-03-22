{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if target.name == 'prod' -%}
        {%- if custom_schema_name is none -%}
            {{ target.schema }}
        {%- else -%}
            {{ custom_schema_name | upper }}
        {%- endif -%}

    {%- else -%}
        {%- if custom_schema_name is none -%}
            {{ target.schema | upper }}
        {%- else -%}
            {{ target.schema | upper }}_{{ custom_schema_name | upper }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}