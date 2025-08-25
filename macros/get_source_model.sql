{% macro get_source_model_entries(name, dv_entity) %}
	-- This macro retrieves all metadata entries as a list of dictionaries.
	-- It is designed for structures like:
	-- [{"bk_columns": ["n_nationkey"], "hk_column": "hk_nation_h", ...}]

	-- 1. Determine which table to query based on the dv_entity.

	{% set ref_table = '' %}
	{% set where_column = '' %}

	{% if dv_entity == 'hub' %}
			{% set ref_table = 'hub_metadata' %}
			{% set where_column = 'hub_name' %}
	{% elif dv_entity == 'link' %}
		{% set ref_table = 'link_metadata' %}
		{% set where_column = 'link_name' %}
	{% elif dv_entity == 'nh_link' %}
		{% set ref_table = 'nh_link_metadata' %}
		{% set where_column = 'link_name' %}
	{% else %}
		-- Return an empty list if the dv_entity is not valid.
		{{ return([]) }}
	{% endif %}

	{% set latest_id = get_latest_invocation_id(ref_table) %}

	-- 2. Construct the query dynamically.
	{% set query %}
		SELECT source_models
		FROM {{ ref(ref_table) }}
		WHERE {{ where_column }} = '{{ name }}'
		AND invocation_id = '{{ latest_id }}'
	{% endset %}

	{% set results = run_query(query) %}

	-- 3. Check if the query returned a result.
	{% if execute and results and results.rows %}

		-- 4. Extract the JSON string and parse it into a list of dictionaries.
		{% set metadata_string = results.rows[0][0] %}
		{% set metadata_list = fromjson(metadata_string) %}

		-- 5. Return the full list of dictionary objects.
		{{ return(metadata_list) }}

	{% else %}
		-- Return an empty list if the query fails.
		{{ return([]) }}
	{% endif %}

{% endmacro %}
