  -- depends_on: {{ ref('stage_metadata') }}

{% set hub_query %}
  SELECT DISTINCT hub_name, hub_hashkey, business_keys
  FROM {{ ref('hub_metadata') }}
{% endset %}

{% if execute %}
  {% set results = run_query(hub_query) %}
{% endif %}

with source_models as (
    SELECT DISTINCT
        hub_name,
        hub_hashkey,
        business_keys,
        CASE
            {% if execute and results and results.rows %}
                {% for row in results.rows %}
                    {% set hub_name_val = row[0] %}
                    WHEN hub_name = '{{ hub_name_val }}'
                    THEN {{ get_source_models_hub(hub_name_val) }}
                {% endfor %}
            {% endif %}
            ELSE NULL
        END AS source_models_list_hub
    FROM {{ ref('hub_metadata') }}
)
,



final_output as (
    SELECT
        hub_name,
        hub_hashkey,
        business_keys,
        source_models_list_hub,
        CASE
            {% if execute and results and results.rows %}
                {% for row in results.rows %}
                    {% set hub_name_val = row[0] %}
                    {% set hub_hashkey_val = row[1] %}
                    {% set business_keys = row[2] %}
                    {% set stages_list = get_source_models_hub(hub_name_val) %}
                    {% for stage in stages_list %}
                        {% set hashed_columns_dict = get_hashed_column_values(stage) %}
                        {% if hub_hashkey_val in hashed_columns_dict %}
                            WHEN hub_name = '{{ hub_name_val }}'
                            {% set hashkey_inputs = hashed_columns_dict[hub_hashkey_val] | join(', ') %}
                            {{ log("Value for " ~ hub_hashkey_val ~ " is: " ~ hashkey_inputs, info=true) }}
                            THEN '{{ hashkey_inputs }}'
                        {% endif %}
                    {% endfor %}
                {% endfor %}
            {% endif %}
            ELSE NULL
        END AS hashkey_input
    FROM source_models
)

SELECT 
*
    FROM final_output
WHERE business_keys!=hashkey_input


