{# 
{% set hub_names_query %}
  SELECT DISTINCT hub_name,hub_hashkey
  FROM {{ ref('hub_metadata') }}
{% endset %}

{% if execute %}
  {% set results = run_query(hub_names_query) %}
{% else %}
  {% set results = [] %}
{% endif %}

with source_models as (

SELECT
    hub_name,
    hub_hashkey,
    business_keys,
    CASE
        {% if execute and results and results.rows %}
            {% for row in results.rows %}
                {% set hub_name_val = row[0] | trim %}
                WHEN hub_name = '{{ hub_name_val }}'
                    THEN {{ get_source_models_hub(hub_name_val) }}
            {% endfor %}
        {% endif %}
        ELSE NULL
    END AS source_models_list_hub
FROM {{ ref('hub_metadata') }} ),

final_output as (
SELECT 
    hub_name,
    hub_hashkey,
    business_keys,
    source_models_list_hub,
        CASE
        {% if execute and results and results.rows %}
            {% for row in results.rows %}
                {% set hub_hashkey_val = row[1] | trim %}
                WHEN hub_name = '{{ hub_hashkey_val }}'
                    THEN {{get_hashed_column_values('source_models_list_hub', 'hub_hashkey_val')}}
            {% endfor %}
        {% endif %}
        ELSE NULL
    END AS hashkey

 FROM source_models )







  
 #}
