{%- macro insert_metadata_global_vars() -%}

{%- set ldts_alias = var('datavault4dbt.ldts_alias',  'Not_Defined') -%}
{%- set rsrc_alias = var('datavault4dbt.rsrc_alias',  'Not_Defined') -%}
{%- set ledts_alias = var('datavault4dbt.ledts_alias',  'Not_Defined') -%}
{%- set sdts_alias = var('datavault4dbt.sdts_alias',  'Not_Defined') -%}
{%- set snapshot_trigger_column = var('datavault4dbt.snapshot_trigger_column',  'Not_Defined') -%}
{%- set stg_alias = var('datavault4dbt.stg_alias',  'Not_Defined') -%}
{%- set is_current_col_alias = var('datavault4dbt.is_current_col_alias',  'Not_Defined') -%}
{%- set is_active_alias = var('datavault4dbt.is_active_alias',  'Not_Defined') -%}
{%- set default_unknow_rsrc = var('datavault4dbt.default_unknow_rsrc',  'Not_Defined') -%}
{%- set default_error_rsrc = var('datavault4dbt.default_error_rsrc',  'Not_Defined') -%}
{%- set hash = var('datavault4dbt.hash',  'Not_Defined') -%}
{%- set hash_datatype = var('datavault4dbt.hash_datatype',  'Not_Defined') -%}
{%- set hashkey_input_case_sensitivity = var('datavault4dbt.hashkey_input_case_sensitivity',  'FALSE') -%}
{%- set hashdiff_input_case_sensitivity = var('datavault4dbt.hashdiff_input_case_sensitivity',  'TRUE') -%}
{%- set copy_rsrs_ldts_input_columns = var('datavault4dbt.copy_rsrs_ldts_input_columns',  'FALSE') -%}

    {% set query %}
        INSERT INTO {{ get_model_db_name_dict('global_vars_columns_metadata').full_name }} (
        execution_timestamp,
        ldts_alias,
        rsrc_alias,
        ledts_alias,
        sdts_alias,
        snapshot_trigger_column,
        stg_alias,
        is_current_col_alias,
        is_active_alias,
        beginning_of_all_times,
        end_of_all_times,
        timestamp_format,
        beginning_of_all_times_date,
        end_of_all_times_date,
        date_format,
        default_unknow_rsrc,
        default_error_rsrc,
        rsrc_default_dtype,
        stg_default_dtype,
        derived_columns_default_dtype,
        hash,
        hash_datatype,
        hashkey_input_case_sensitivity,
        hashdiff_input_case_sensitivity,
        copy_rsrs_ldts_input_columns,
        invocation_id 
        )
    SELECT
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST( '{{ ldts_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ldts_alias,
        CAST( '{{ rsrc_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS rsrc_alias,
        CAST( '{{ ledts_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ledts_alias,
        CAST( '{{ sdts_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS sdts_alias,
        CAST( '{{ snapshot_trigger_column }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS snapshot_trigger_column,
        CAST( '{{ stg_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS stg_alias,
        CAST( '{{ is_current_col_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS is_current_col_alias,
        CAST( '{{ is_active_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS is_active_alias,
        CAST( '{{ datavault4dbt.beginning_of_all_times() }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS beginning_of_all_times,
        CAST( '{{ datavault4dbt.end_of_all_times() }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS end_of_all_times,
        CAST( '{{ datavault4dbt.timestamp_format() }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS timestamp_format,
        CAST( '{{ datavault4dbt.beginning_of_all_times_date() }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS beginning_of_all_times_date,
        CAST( '{{ datavault4dbt.end_of_all_times_date() }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS end_of_all_times_date,
        CAST( '{{ datavault4dbt.date_format() }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS date_format,
        CAST( '{{ default_unknow_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS default_unknow_rsrc,
        CAST( '{{ default_error_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS default_error_rsrc,
        CAST( '{{ datavault4dbt.string_default_dtype(type='rsrc') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS rsrc_default_dtype,
        CAST( '{{ datavault4dbt.string_default_dtype(type='stg') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS stg_default_dtype,
        CAST( '{{ datavault4dbt.string_default_dtype(type='derived_columns') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS derived_columns_default_dtype,
        CAST( '{{ hash }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS hash,
        CAST( '{{ hash_datatype }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS hash_datatype,
        CAST( '{{ hashkey_input_case_sensitivity }}' AS BOOLEAN) AS hashkey_input_case_sensitivity,
        CAST( '{{ hashdiff_input_case_sensitivity }}' AS BOOLEAN) AS hashdiff_input_case_sensitivity,
        CAST( '{{ copy_rsrs_ldts_input_columns }}' AS BOOLEAN) AS copy_rsrs_ldts_input_columns,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id 
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}