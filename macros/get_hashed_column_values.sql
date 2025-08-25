{% macro get_hashed_column_values(stage_model) %}

    {% set query %}
        SELECT hashed_columns
        FROM {{ ref('stage_metadata') }}
        WHERE stage_name = '{{ stage_model }}'
        AND invocation_id = {{ get_latest_invocation_id('stage_metadata') }}
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
