{# -- This macro retrieves the 'hashed_columns' string for a specified stage model
-- and returns the value of a specific key as a Jinja list.
--
-- To use this macro in a dbt model, you would call it like this:
-- {% set columns = get_hashed_column_values('stg_lineitem', 'hk_lineitem_nl') %}
--
-- This would return a Jinja list like ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber'].
--
-- @param stage_model (string): The name of the stage model to query from `stage_metadata`.
-- @param key_to_return (string): The key in the dictionary whose value you want to retrieve.

{% macro get_hashed_column_values(stage_model, key_to_return) %}

  -- 1. Use `run_query` to fetch the dictionary string from the database.
  {% set query %}
    SELECT hashed_columns
    FROM {{ ref('stage_metadata') }}
    WHERE stage_name = '{{ stage_model }}'
  {% endset %}

  {% set results = run_query(query) %}

  -- 2. Check if a result was returned and execute the logic.
  {% if execute and results and results.rows %}

    -- 3. Extract the string and use `fromjson` to parse it into a dictionary.
    {% set hashed_columns_string = results.rows[0][0] %}
    {% set hashed_columns_dict = fromjson(hashed_columns_string) %}

    -- 4. Check if the specified key exists in the dictionary.
    {% if key_to_return in hashed_columns_dict %}
      -- 5. Return the value associated with the specified key.
      -- The value is already a list, so we just return it directly.
      {{ return({ key_to_return: hashed_columns_dict[key_to_return] }) }}
    {% else %}
      -- If the key is not found, return an empty list.
      {{ return(None) }}
    {% endif %}

  {% else %}
    -- Return an empty list if the query fails or returns no results.
    {{ return(None) }}
  {% endif %}

{% endmacro %} #}


{% macro get_hashed_column_values(stage_model) %}

  {% set query %}
    SELECT hashed_columns
    FROM {{ ref('stage_metadata') }}
    WHERE stage_name = '{{ stage_model }}'
  {% endset %}

  {% set results = run_query(query) %}

  {% if execute and results and results.rows %}
    {% set hashed_columns_string = results.rows[0][0] %}
    {% set hashed_columns_dict = fromjson(hashed_columns_string) %}
    {{ return(hashed_columns_dict) }}
  {% else %}
    {{ return(None) }}
  {% endif %}

{% endmacro %}

