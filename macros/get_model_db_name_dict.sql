{%- macro get_model_db_name_dict(model_name) -%}
  {% set ns = namespace(model_dict={}) %}
  {%- if graph.nodes is defined -%} {# graph is empty when running unit tests and that caused an error for graph.nodes #}
    {%- if execute -%}
      {%- for node in graph.nodes.values() | selectattr("name", "equalto", model_name) -%}
        {%- set ns.model_dict = {
          "database": node.database,
          "schema": node.schema,
          "name": node.name,
          "full_name": node.database ~ '.' ~ node.schema ~ '.' ~ node.name
        } %}
      {%- endfor -%}
    {%- endif -%}
  {%- else -%}
    {%- set query -%}
      select
        object_construct(
          'database', table_catalog,
          'schema', table_schema,
          'name', table_name,
          'full_name', concat_ws('.', table_catalog, table_schema, table_name)
        )
      from
        information_schema.tables
      where true
        and table_schema ilike '{{ target.schema }}'
        and table_name ilike '{{ model_name }}'
    {%- endset -%}
    {% if execute %}
      {%- set results = run_query(query) -%}
      {%- set ns.model_dict = results.columns[0].values()[0] -%}
    {%- else -%}
      {%- set ns.model_dict = {} -%}
    {%- endif -%}
  {%- endif -%}
  {{ return(ns.model_dict) }}
{%- endmacro -%}
