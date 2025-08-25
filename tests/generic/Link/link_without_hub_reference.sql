{% test link_without_hub_references(model) %}

{% set link_metadata_query %}
  -- 1. Get all links and their foreign hashkeys from the link_metadata table.
  SELECT DISTINCT
    link_name,
    foreign_hashkeys
  FROM {{ ref('link_metadata') }}
  WHERE foreign_hashkeys IS NOT NULL
	AND invocation_id = {{ get_latest_invocation_id('link_metadata') }}
{% endset %}

{% set hub_hashkeys_query %}
  -- 2. Get a list of all valid hub hashkeys from the hub_metadata table.
  SELECT DISTINCT
    hub_hashkey
  FROM {{ ref('hub_metadata') }}
{% endset %}

{% set link_results = run_query(link_metadata_query) %}
{% set hub_results = run_query(hub_hashkeys_query) %}

{% set hub_hashkeys = [] %}
{% set failing_references = [] %}

-- 3. Populate the list of all valid hub hashkeys.
{% if execute and hub_results and hub_results.rows %}
  {% for row in hub_results.rows %}
    {% do hub_hashkeys.append(row[0] | trim) %}
  {% endfor %}
{% endif %}

-- 4. Iterate through each link and check its foreign hashkeys.
{% if execute and link_results and link_results.rows %}
  {% for row in link_results.rows %}
    {% set link_name = row[0] | trim %}
    {% set foreign_hashkeys_string = row[1] | trim %}
    
    -- Split the string into a list of foreign hashkeys.
    {% set foreign_hashkeys = foreign_hashkeys_string.split(',') | map('trim') %}

    -- Check each foreign hashkey against the list of valid hubs.
    {% for fk in foreign_hashkeys %}
      {% if fk not in hub_hashkeys %}
        {% set error_message = "Foreign key ''" ~ fk ~ "'' in link ''" ~ link_name ~ "'' does not refer to an existing hub." %}
        {% do failing_references.append({'link_name': link_name, 'fk': fk, 'error_message': error_message}) %}
      {% endif %}
    {% endfor %}
  {% endfor %}
{% endif %}

-- 5. Generate the final SQL query to show the failing rows.
{% if failing_references %}
  {% for failure in failing_references %}
  SELECT DISTINCT
    '{{ failure['error_message'] }}' AS error_message,
    '{{ failure['link_name'] }}' AS link_name,
    '{{ failure['fk'] }}' AS foreign_hashkey
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
    NULL AS foreign_hashkey
  WHERE 1=0
{% endif %}

{% endtest %}