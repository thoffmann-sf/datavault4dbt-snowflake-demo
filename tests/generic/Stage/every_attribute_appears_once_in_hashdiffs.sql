{# 
Test: Ensure every hashed column only appears once across all stages, 
consider only entries where is_hashdiff = true.
#}

-- depends_on: {{ ref('stage_metadata') }}

{% test every_attribute_appears_once_in_hashdiffs(model) %}
{{
    config(severity = 'warn')
}}

{% set stage_query %}
    SELECT DISTINCT
        stage_name,
        hashed_columns
    FROM {{ ref('stage_metadata') }}
    WHERE stage_name IS NOT NULL
    AND invocation_id = {{ get_latest_invocation_id('stage_metadata') }}
{% endset %}

{% set stage_results = run_query(stage_query) %}
{% set all_columns = [] %}
{% set duplicates = [] %}

{% if execute and stage_results and stage_results.rows %}
  {% for stage_row in stage_results.rows %}
    {% set stage_name = stage_row[0] %}
    {% set hashed_dict = fromjson(stage_row[1]) %}

    {{ log("stage_name: " ~ stage_name, true) }}
    {{ log("hashed_dict: " ~ hashed_dict, true) }}

    {# Iterate through all entries in hashed_columns #}
    {% for key, value in hashed_dict.items() %}
      {% if value is mapping and value.get('is_hashdiff') %}
        {% for col in value.get('columns') %}
          {% if col in all_columns and col not in duplicates %}
            {% do duplicates.append({"stage_name": stage_name, "column": col}) %}
          {% else %}
            {% do all_columns.append(col) %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endfor %}
  {% endfor %}
{% endif %}

{# Final SQL output #}
{% if duplicates %}
  {% for dup in duplicates %}
    SELECT DISTINCT
      'Duplicate input column for hashdiffs across this stage' AS error_message,
      '{{ dup["stage_name"] }}' AS stage_name,
      '{{ dup["column"] }}' AS duplicate_column
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
{% else %}
  SELECT
    NULL AS error_message,
    NULL AS stage_name,
    NULL AS duplicate_column
  WHERE 1=0
{% endif %}

{% endtest %}

