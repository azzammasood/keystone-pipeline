{#- No package dependencies: a small portable version of dbt_utils.unique_combination_of_columns. -#}
{% test unique_combination(model, columns) %}
SELECT {{ columns | join(', ') }}
FROM {{ model }}
GROUP BY {{ columns | join(', ') }}
HAVING COUNT(*) > 1
{% endtest %}
