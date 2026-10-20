{#- Schemas are named by layer (staging, intermediate, marts, metrics, ref), not prefixed with
    the target schema, so every engine and the Iceberg publish see the same names. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
