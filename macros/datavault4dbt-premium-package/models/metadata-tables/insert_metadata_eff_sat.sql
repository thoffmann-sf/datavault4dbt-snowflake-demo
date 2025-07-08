{%- macro insert_metadata_eff_sat(source_model, tracked_hashkey, src_ldts, src_rsrc, is_active_alias, source_is_single_batch, disable_hwm) -%}

{{ log('Test ob EFF Macro Call geht', info=True) }}
    {% set query %}
        INSERT INTO {{this.database}}.dbt_thoffmannsf_datavault4dbt_premium_package.eff_sat_metadata (
            Sat_Name,
            Tracked_Hashkey,
            source_model,
            disable_hwm,
            src_ldts,
            src_rsrc,
            source_is_single_batch,
            is_active_alias,
            execution_timestamp,
            invocation_id
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_Name,
        CAST('{{ tracked_hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Tracked_Hashkey,
        CAST('{{ source_model }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_model,
        CAST('{{ disable_hwm }}' AS BOOLEAN) AS disable_hwm,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST('{{ source_is_single_batch }}' AS BOOLEAN) AS source_is_single_batch,
        CAST('{{ is_active_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS is_active_alias,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}