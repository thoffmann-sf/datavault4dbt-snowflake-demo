-- depends_on: {{ ref('stage_metadata') }}

{% set link_metadata_query %}
    SELECT DISTINCT
        link_name,
        link_hashkey,
        foreign_hashkeys,
        source_models
    FROM {{ ref('link_metadata') }}
    WHERE foreign_hashkeys IS NOT NULL
{% endset %}

{% set link_results = run_query(link_metadata_query) %}
{% set failing_links = [] %}


{% if execute and link_results and link_results.rows %}
    {% for row in link_results.rows %}
        {% set link_name = row[0] | trim %}
        {% set link_hashkey = row[1] | trim %}
        {% set foreign_hashkeys_string = row[2] | trim %}
        {% set stage_model = row[3] | trim %}
        {% set foreign_keys = foreign_hashkeys_string.split(',') | map('trim') | list %}

        -- Get the full dictionary of hashed columns for the stage.
        {% set hashed_columns_dict = get_hashed_column_values(stage_model) %}

        -- Get the columns for the link's hashkey.
        {% set link_hashkey_columns = [] %}
        {% if hashed_columns_dict and link_hashkey in hashed_columns_dict %}
            {% set link_hashkey_columns = hashed_columns_dict[link_hashkey] %}
        {% endif %}

        -- Get the combined list of columns for all foreign keys.
        {% set combined_fk_columns = [] %}
        {% for fk in foreign_keys %}
            {% if hashed_columns_dict and fk in hashed_columns_dict %}
                {% do combined_fk_columns.extend(hashed_columns_dict[fk]) %}
            {% endif %}
        {% endfor %}

        -- Compare the two lists. We sort them to ensure the comparison is order-independent.
        {% if combined_fk_columns | sort != link_hashkey_columns | sort %}
            {% set error_message = "Link hashkey columns for '" ~ link_name ~ "' do not match combined foreign key inputs." %}
            {% do failing_links.append({
                'link_name': link_name,
                'link_hashkey': link_hashkey,
                'expected_columns': combined_fk_columns,
                'actual_columns': link_hashkey_columns,
                'error_message': error_message
            }) %}
        {% endif %}
    {% endfor %}
{% endif %}


-- Generate the final SQL query to show the failing rows.
{% if failing_links %}
    {% for failure in failing_links %}
        SELECT
            '{{ failure['error_message'] }}' AS error_message,
            '{{ failure['link_name'] }}' AS link_name,
            '{{ failure['link_hashkey'] }}' AS link_hashkey,
            '{{ failure['expected_columns'] | tojson }}' AS expected_columns,
            '{{ failure['actual_columns'] | tojson }}' AS actual_columns
        FROM {{ ref('link_metadata') }}
        WHERE link_name = '{{ failure['link_name'] }}'

        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
{% else %}
    -- If no failures are found, return an empty result set.
    SELECT
        NULL AS error_message,
        NULL AS link_name,
        NULL AS link_hashkey,
        NULL AS expected_columns,
        NULL AS actual_columns
    WHERE 1=0
{% endif %}
{# 
Into the link and see link_name,link_hashkey,foreign_hashkeys and source_model [customer_nation_l,hk_customer_nation_l,[hk_customer_h, hk_nation_h],"stg_customer"]
then go into the stage_metadata and get the hashed columns for the link_hashkey where the stage_name=SOURCE_MODELS
(stg_customer="stg_customer")
With my macro query the values for the key=link_hashkey "hk_customer_nation_l": ["c_custkey", "c_nationkey"], 
with my macro query the values for the each foreign_hashkeys [hk_customer_h= c_custkey, hk_nation_h=c_nationkey]
Then compare the foreign_hashkeys with the link_hashkey columns "hk_customer_nation_l": ["c_custkey", "c_nationkey"],  are those values both present in [hk_customer_h= c_custkey, hk_nation_h=c_nationkey]
if two inputs for the link_hashkey, then i need to find both inputs in the hashed columns for the foreign_hashkeys as an input
 #}

 