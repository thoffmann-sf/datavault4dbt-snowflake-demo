-- depends_on: {{ ref('stage_metadata') }}

{# 
For each link:
- Get link_name from link_metadata
- Get source_model_entries (which include stage name, link_hk, fk_columns)
- From stage_metadata, fetch hashed columns for that stage
- Compare:
    link_hk_input (inputs to build link_hk)
    combined_fk_input_columns (union of inputs for all fk_columns)
- If mismatch, log error
#}

{% set link_query %}
  SELECT DISTINCT link_name
  FROM {{ ref('link_metadata') }}
  WHERE link_name IS NOT NULL
{% endset %}

{% set link_results = run_query(link_query) %}
{% set failing_links = [] %}

{% if execute and link_results and link_results.rows %}
  {% for link_row in link_results.rows %}
    {% set link_name = link_row[0] | trim %}

    {% set source_model_entries = get_source_model_entries(link_name, 'link') %}

    {% for sm_entry in source_model_entries %}
      {% set stage_name = sm_entry['name'] %}
      {% set link_hashkey = sm_entry['link_hk'] %}
      {% set foreign_hashkeys = sm_entry['fk_columns'] %}

      {{ log("stage_name: " ~ stage_name, true) }}
      {{ log("link_hashkey: " ~ link_hashkey, true) }}
      {{ log("foreign_hashkeys: " ~ foreign_hashkeys, true) }}

      {% set stage_hashed_columns = get_hashed_column_values(stage_name) %}
      {{ log("stage_hashed_columns: " ~ stage_hashed_columns, true) }}

      {% if stage_hashed_columns is not none %}
        {# inputs used for building the link hashkey #}
        {% set link_hk_input = stage_hashed_columns[link_hashkey] | sort %}
        {{ log("link_hk_input: " ~ link_hk_input, true) }}

        {# union of inputs for all foreign hashkeys #}
        {% set combined_fk_input_columns = [] %}
        {% for fk in foreign_hashkeys %}
          {% if fk in stage_hashed_columns %}
            {% do combined_fk_input_columns.extend(stage_hashed_columns[fk]) %}
          {% endif %}
        {% endfor %}
        {% set combined_fk_input_columns = combined_fk_input_columns | sort %}
        {{ log("combined_fk_input_columns: " ~ combined_fk_input_columns, true) }}

        {# compare link_hk vs combined foreign_hk inputs #}
        {% if link_hk_input != combined_fk_input_columns %}
          {% set error_message = "Mismatched hashkey inputs for link \"" ~ link_name ~ "\" from stage \"" ~ stage_name ~ "\"." %}
          {% do failing_links.append({ 
            'link_name': link_name,
            'stage_name': stage_name,
            'link_hashkey': link_hashkey,
            'foreign_hashkeys': foreign_hashkeys,
            'expected_columns': combined_fk_input_columns,
            'actual_columns': link_hk_input,
            'error_message': error_message
          }) %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endfor %}
{% endif %}

{# Final SQL output #}
{% if failing_links %}
  {% for failure in failing_links %}
    SELECT DISTINCT
      '{{ failure['error_message'] }}' AS error_message,
      '{{ failure['link_name'] }}' AS link_name,
      '{{ failure['stage_name'] }}' AS stage_name,
      '{{ failure['link_hashkey'] }}' AS link_hashkey,
      '{{ failure['foreign_hashkeys'] | tojson }}' AS foreign_hashkeys,
      '{{ failure['expected_columns'] | tojson }}' AS expected_columns,
      '{{ failure['actual_columns'] | tojson }}' AS actual_columns
    FROM {{ ref('link_metadata') }}
    WHERE link_name = '{{ failure['link_name'] }}'
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
{% else %}
  SELECT
    NULL AS error_message,
    NULL AS link_name,
    NULL AS stage_name,
    NULL AS link_hashkey,
    NULL AS foreign_hashkeys,
    NULL AS expected_columns,
    NULL AS actual_columns
  WHERE 1=0
{% endif %}
