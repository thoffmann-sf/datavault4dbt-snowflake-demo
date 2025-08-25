{% macro get_latest_invocation_id(ref_table) %}
    {# 
        Returns the invocation_id of the latest execution for a given table 
    #}

    {% set query %}
				(SELECT invocation_id
        FROM {{ ref(ref_table) }}
        ORDER BY execution_timestamp DESC
        LIMIT 1 )
    {% endset %}

    {{ return(query) }}
{% endmacro %}
