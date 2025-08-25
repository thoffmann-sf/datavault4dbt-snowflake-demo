{% test sats_not_in_pit(model) %}

{% set all_sats_query %}
  SELECT DISTINCT
    sat_name
  FROM {{ ref('sat_v0_metadata') }}
  WHERE invocation_id = get_latest_invocation_id('sat_v0_metadata')
{% endset %}

{% set all_sats_results = run_query(all_sats_query) %}

{% set pit_sats_query %}
  SELECT DISTINCT
    sat_names
  FROM {{ ref('pit_metadata') }}
  WHERE invocation_id = {{ get_latest_invocation_id('pit_metadata') }}
{% endset %}

{% set pit_sats_results = run_query(pit_sats_query) %}

{% set all_satellites = [] %}
{% set pit_satellites = [] %}
{% set unused_satellites = [] %}

{% if execute and all_sats_results and all_sats_results.rows %}
  {% for row in all_sats_results.rows %}
    {% do all_satellites.append(row[0] | trim) %}
  {% endfor %}
{% endif %}

{% if execute and pit_sats_results and pit_sats_results.rows %}
  {% for row in pit_sats_results.rows %}
    {% set sat_names_string = row[0] | trim %}
    {% do pit_satellites.extend(sat_names_string.split(', ')) %}
  {% endfor %}
{% endif %}

{% for satellite in all_satellites %}
  {% if satellite not in pit_satellites %}
    {% do unused_satellites.append(satellite) %}
  {% endif %}
{% endfor %}

{% if unused_satellites %}
  {% for sat in unused_satellites %}
  SELECT
    'Satellite {{ sat }} is not used in any PIT table.' AS error_message,
    '{{ sat }}' AS satellite_name
  FROM {{ ref('sat_v0_metadata') }}
  -- This WHERE clause ensures that the test returns exactly one row per failure
  WHERE sat_name = '{{ sat }}'
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
{% else %}
  SELECT
    NULL AS error_message,
    NULL AS satellite_name
  WHERE 1=0
{% endif %}

{% endtest %}