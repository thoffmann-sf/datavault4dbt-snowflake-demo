{% set link_metadata_query %}
  SELECT DISTINCT
    link_name,
    foreign_hashkeys
  FROM {{ ref('link_metadata') }}
  WHERE foreign_hashkeys IS NOT NULL
{% endset %}

{% set results = run_query(link_metadata_query) %}

{% set failing_links = [] %}

{% if execute and results and results.rows %}
  {% for row in results.rows %}
    {% set link_name = row[0] | trim %}
    {% set foreign_hashkeys_string = row[1] | trim %}
    
    {% set foreign_hashkeys = foreign_hashkeys_string.split(', ') %}

    {% if foreign_hashkeys | length < 2 %}
      {% do failing_links.append({'link_name': link_name, 'fk_count': foreign_hashkeys | length}) %}
    {% endif %}
  {% endfor %}
{% endif %}

{% if failing_links %}
  {% for link in failing_links %}
    SELECT
      'The link ''{{ link['link_name'] }}'' has only {{ link['fk_count'] }} foreign hashkey(s).' AS error_message,
      '{{ link['link_name'] }}' AS link_name,
      {{ link['fk_count'] }} AS fk_count
    {% if not loop.last %}
    UNION ALL
    {% endif %}
  {% endfor %}
{% else %}

  SELECT
    NULL AS error_message,
    NULL AS link_name,
    NULL AS fk_count
  WHERE 1=0
{% endif %}
