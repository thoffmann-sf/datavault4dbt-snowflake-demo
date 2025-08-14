{# -- depends_on: {{ ref('link_metadata') }}, {{ ref('hub_metadata') }}

{% set link_query %}
    SELECT link_name, link_hashkey, foreign_hashkeys
    FROM {{ ref('link_metadata') }}
{% endset %}

{% set hub_query %}
    SELECT hub_name, hub_hashkey
    FROM {{ ref('hub_metadata') }}
{% endset %}

{% if execute %}
    {% set link_results = run_query(link_query) %}
    {% set hub_results = run_query(hub_query) %}

    {% set links_dict = {} %}
    {% for row in link_results.rows %}
        {% do links_dict.update({
            row[0]: {
                "link_name": row[0],
                "link_hashkey": row[1],
                "foreign_hashkeys": row[2]
            }
        }) %}
    {% endfor %}

    {% set hubs_dict = {} %}
    {% for row in hub_results.rows %}
        {% do hubs_dict.update({
            row[0]: {
                "hub_name": row[0],
                "hub_hashkey": row[1]
            }
        }) %}
    {% endfor %}

    {% set link_hub_list = [] %}
    {% for link_name, link_data in links_dict.items() %}
        {% set foreign_hks = link_data.foreign_hashkeys.split(', ') %}
        {% for fk in foreign_hks %}
            {% for hub_name, hub_data in hubs_dict.items() %}
                {% if fk == hub_data.hub_hashkey %}
                    {% do link_hub_list.append({
                        "link_name": link_data.link_name,
                        "link_hashkey": link_data.link_hashkey,
                        "foreign_hashkey": fk,
                        "hub_name": hub_data.hub_name,
                        "hub_hashkey": hub_data.hub_hashkey
                    }) %}
                {% endif %}
            {% endfor %}
        {% endfor %}
    {% endfor %}
{% endif %}

-- Final SELECT
with final_data as (
    {% if execute %}
        select
            link_name,
            link_hashkey,
            foreign_hashkey,
            hub_name,
            hub_hashkey
        from (values
            {% for row in link_hub_list %}
                ('{{ row.link_name }}', '{{ row.link_hashkey }}', '{{ row.foreign_hashkey }}', '{{ row.hub_name }}', '{{ row.hub_hashkey }}')
                {% if not loop.last %},{% endif %}
            {% endfor %}
        ) as t(link_name, link_hashkey, foreign_hashkey, hub_name, hub_hashkey)

    {% endif %}
)

select * from final_data
order by link_name, foreign_hashkey #}



SELECT link_name,
       link_hashkey,
       foreign_hashkey,
       hub_name,
       hub_hashkey
    from {{ ref('link_metadata') }}
    left join {{ ref('hub_metadata') }}
    on link_metadata.foreign_hashkeys = hub_metadata.hub_hashkey