{% macro rsrc_concat_stage(string_list=none) -%}
  {{ return(adapter.dispatch('rsrc_concat_stage', 'datavault4dbt_automatic_tests')(string_list)) }}
{%- endmacro -%}

{%- macro default__rsrc_concat_stage(string_list) -%}
  {{ return("CONCAT(record_source, '__', record_source)") }}
{%- endmacro -%}

{%- macro redshift__rsrc_concat_stage(string_list) -%}
  {{ return(adapter.dispatch('concat_ws', 'datavault4dbt')(
    string_list=string_list
  )) }}
{%- endmacro -%}

{%- macro oracle__rsrc_concat_stage(string_list) -%}
  {{ return("record_source || '__' || record_source") }}
{%- endmacro -%}