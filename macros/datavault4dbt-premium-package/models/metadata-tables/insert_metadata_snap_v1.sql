{%- macro insert_metadata_snap_v1(control_snap_v0, log_logic, sdts_alias) -%}
{%- set json_log_logic = tojson(log_logic) -%}
{{ log('Test ob Snap V1 Macro Call geht', info=True) }}
    {% set query %}
        INSERT INTO {{this.database}}.dbt_thoffmannsf_datavault4dbt_premium_package.snap_v1_metadata (
        Snap_name,
        control_snap_v0,
        log_logic,
        sdts_alias,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Snap_name,
        CAST('{{ control_snap_v0 }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS control_snap_v0,
        CAST('{{ json_log_logic }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS log_logic,
        CAST('{{ sdts_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS sdts_alias,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}