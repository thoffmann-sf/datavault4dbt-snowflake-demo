-- This macro queries the 'hub_metadata' table to get a JSON string,
-- extracts the top-level keys from that string, and returns them as a
-- Jinja list.


{% macro get_source_models_hub(hub_name) %}

  -- 1. Query the hub_metadata table to get the JSON string.
  {% set query %}
    SELECT source_models
    FROM {{ ref('hub_metadata') }}
    WHERE hub_name = '{{ hub_name }}'
  {% endset %}

  {% set results = run_query(query) %}

  -- 2. Check if the query returned a result.
  {% if execute and results and results.rows %}

    -- 3. Extract the JSON string and parse it into a dictionary.
    {% set metadata_string = results.rows[0][0] %}
    {% set metadata_dict = fromjson(metadata_string) %}
    
    -- 4. Get the keys from the dictionary and return them as a list.
    -- The output is a Jinja list of keys
    {{ return(metadata_dict.keys() | list) }}

  {% else %}
    -- Return an empty list if the query fails.
    {{ return([]) }}
  {% endif %}

{% endmacro %}
