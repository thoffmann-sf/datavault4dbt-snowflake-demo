{% macro get_latest_invocation_id(ref_table) %}
    {# 
        Returns the invocation_id of the latest execution for a given table 
    #}

    {% set query %}
				SELECT invocation_id
        FROM {{ ref(ref_table) }}
        ORDER BY execution_timestamp DESC
        LIMIT 1
    {% endset %}

    {% set result = run_query(query) %}

    {% if execute and result and result.rows %}
        {% set latest_invocation_id = result.rows[0][0] %}
    {% else %}
        {% set latest_invocation_id = none %}
    {% endif %}

    {{ return(latest_invocation_id) }}
{% endmacro %}
