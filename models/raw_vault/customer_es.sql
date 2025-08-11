{{config(materialized = 'incremental')}}

{%- set yaml_metadata -%}
tracked_hashkey: hk_customer_h
is_active_alias: 'active'
source_model: stg_customer
{%- endset -%}

{{ datavault4dbt.eff_sat_v0(yaml_metadata=yaml_metadata) }}