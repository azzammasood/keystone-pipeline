-- PEP 503 project name: lower case, every run of -, _ or . becomes one -. Python's
-- keystone.resolve.urls.pypi_name is the same rule (a pytest test compares them).
{% macro pypi_normalize(col) %}
    {{ return(adapter.dispatch('pypi_normalize')(col)) }}
{% endmacro %}

{% macro default__pypi_normalize(col) %}
    LOWER(REGEXP_REPLACE({{ col }}, '[-_.]+', '-'))
{% endmacro %}

{% macro duckdb__pypi_normalize(col) %}
    LOWER(REGEXP_REPLACE({{ col }}, '[-_.]+', '-', 'g'))
{% endmacro %}

-- ecosystem/name as the registry compares names (keystone.resolve.urls.package_key).
{% macro canonical_package_key(ecosystem, name) %}
    CASE
        WHEN {{ ecosystem }} = 'PyPI' THEN 'PyPI/' || {{ pypi_normalize(name) }}
        WHEN {{ ecosystem }} = 'NuGet' THEN 'NuGet/' || LOWER({{ name }})
        ELSE {{ ecosystem }} || '/' || {{ name }}
    END
{% endmacro %}
