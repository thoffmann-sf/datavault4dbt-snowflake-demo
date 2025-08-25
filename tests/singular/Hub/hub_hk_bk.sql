-- depends_on: {{ ref('stage_metadata') }}

{# 
    Get the hub_name (for example order_h) and the source models ( for example :[{"bk_columns": ["o_orderkey"], "hk_column": "hk_order_h", "id": 1, "name": "stg_order", "rsrc_static": "TPC_H_SF1.Orders"}, {"bk_columns": ["l_orderkey"], "hk_column": "hk_order_h", "id": 2, "name": "stg_lineitem", "rsrc_static": "TPC_H_SF1.LineItem"}])
    Get the value for bk_columns (for example: o_orderkey, l_orderkey) and the value for hk_column (for example: hk_order_h)
    Get the name of the corresponding stage (name: stg_order)

    Look into the stage_metadata mentioned below name of the hub (from stage_metadata where stage_name = name: stg_order)
    get the values for every hk_column and check

    Do this for every entry in the source_models, so just for every dict inside this list
#}

{% set hub_query %}
    SELECT DISTINCT hub_name
    FROM {{ ref('hub_metadata') }}
    WHERE hub_name IS NOT NULL
{% endset %}

{% set hub_results = run_query(hub_query) %}
{% set failing_tests = [] %}

{% if execute and hub_results and hub_results.rows %}
    {% for hub_row in hub_results.rows %}
        {% set hub_name = hub_row[0] | trim %}
        
        {% set source_model_entries = get_source_model_entries(hub_name, 'hub') %}
        
        {% for sm_entry in source_model_entries %}
            {% set stage_name = sm_entry['name'] %}
            {{ log('stage_name: '~stage_name, false) }}
            {% set hub_bk_columns = sm_entry['bk_columns'] | sort %}
            {{ log('hub_bk_columns: '~hub_bk_columns, false) }}
            {% set hub_hk_columns = sm_entry['hk_column'] %}
            {{ log('hub_hk_columns: '~hub_hk_columns, false) %}

            {% set stage_hashed_columns = get_hashed_column_values(stage_name) %}
            {{ log('stage_hashed_columns: '~stage_hashed_columns, false) }}
            {% if stage_hashed_columns is not none %}
                {% set stage_bk_columns = stage_hashed_columns[hub_hk_columns] | sort %}
                {{ log('stage_bk_columns: '~stage_bk_columns, false) }}
                {% if hub_bk_columns | sort != stage_bk_columns %}
                    {% set error_message = "Mismatched hashkey inputs for hub \"" ~ hub_name ~ "\" from stage \"" ~ stage_name ~ "\"." %}
                    {% do failing_tests.append({
                        'hub_name': hub_name,
                        'stage_name': stage_name,
                        'hub_hk_columns': hub_hk_columns,
                        'hub_bk_columns': hub_bk_columns,
                        'stage_bk_columns': stage_bk_columns,
                        'error_message': error_message
                    }) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endfor %}
{% endif %}

{% if failing_tests %}
    {% for failure in failing_tests %}
        SELECT DISTINCT
            '{{ failure['error_message'] }}' AS error_message,
            '{{ failure['hub_name'] }}' AS hub_name,
            '{{ failure['stage_name'] }}' AS stage_name,
            '{{ failure['hub_hk_columns'] }}' AS hub_hk_columns,
            '{{ failure['hub_bk_columns'] | tojson }}' AS hub_bk_columns,
            '{{ failure['stage_bk_columns'] | tojson }}' AS stage_bk_columns
        FROM {{ ref('hub_metadata') }}
        WHERE hub_name = '{{ failure['hub_name'] }}'
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
{% else %}
    SELECT NULL as error_message, NULL as hub_name, NULL as stage_name, NULL as hub_hk_columns, NULL as hub_bk_columns, NULL as stage_bk_columns
    WHERE 1=0
{% endif %}
