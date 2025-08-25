{# 
Look into sat_v0_metadata and get for every sat_name the hashdiff and the payload of the sat
then go into the stage and hashed columns, search the hashdiff as a key and compare the values sorted with the payload
 #}


 -- depends_on: {{ ref('stage_metadata') }}



{% set sat_query %}
    SELECT DISTINCT sat_name,hashdiff,payload,source_model
    FROM {{ ref('sat_v0_metadata') }}
    WHERE sat_name IS NOT NULL
    AND invocation_id = get_latest_invocation_id('sat_v0_metadata') 
{% endset %}

{% set sat_results = run_query(sat_query) %}
{% set failing_sats = [] %}

{% if execute and sat_results and sat_results.rows %}
  {% for sat_row in sat_results.rows %}
    {% set sat_name = sat_row[0] | trim %}
    {% set sat_hashdiff = sat_row[1] %}
    {% set sat_payload = sat_row[2]  %}
    {% set sat_payload = sat_payload.split(', ') | sort %}
    {% set source_model= sat_row[3]%}

      {{ log("sat_name: " ~ sat_name, true) }}
      {{ log("sat_hashdiff: " ~ sat_hashdiff, true) }}
      {{ log("sat_payload: " ~ sat_payload, true) }}
      {{ log("source_model: " ~ source_model, true) }}

      {% set stage_hashed_columns = get_hashed_column_values(source_model) %}

      {{ log("stage_hashed_columns: " ~ stage_hashed_columns, true) }}

      {% if stage_hashed_columns is not none %}
        {# inputs used for building the sat hashkey #}
        {% set sat_hd_input = stage_hashed_columns[sat_hashdiff] %}
        {% set sat_hd_input = sat_hd_input['columns'] | sort %}
        {{ log("sat_hd_input: " ~ sat_hd_input, true) }}

        {# compare sat_hk vs combined foreign_hk inputs #}
        {% if sat_hd_input != sat_payload %}
          {% set error_message = "Mismatched hashkey inputs for sat \"" ~ sat_name ~ "\" from stage \"" ~ stage_name ~ "\"." %}
          {% do failing_sats.append({
            'sat_name': sat_name,
            'stage_name': stage_name,
            'sat_hashdiff': sat_hashdiff,
            'sat_hd_input': sat_hd_input,
            'sat_payload': sat_payload,
            'error_message': error_message
          }) %}
        {% endif %}
      {% endif %}
  {% endfor %}
{% endif %}

{# Final SQL output #}
{% if failing_sats %}
  {% for failure in failing_sats %}
    SELECT DISTINCT
      '{{ failure['error_message'] }}' AS error_message,
      '{{ failure['sat_name'] }}' AS sat_name,
      '{{ failure['stage_name'] }}' AS stage_name,
      '{{ failure['sat_hashdiff'] }}' AS sat_hashdiff,
      '{{ failure['sat_hd_input'] | tojson }}' AS sat_hd_input,
      '{{ failure['sat_payload'] | tojson }}' AS sat_payload
    FROM {{ ref('sat_v0_metadata') }}
    WHERE sat_name = '{{ failure['sat_name'] }}'
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
{% else %}
  SELECT
    NULL AS error_message,
    NULL AS sat_name,
    NULL AS stage_name,
    NULL AS sat_hashdiff,
    NULL AS sat_hd_input,
    NULL AS sat_payload
  WHERE 1=0
{% endif %}
