{%- macro insert_metadata_ref_table(ref_hub, ref_satellites, src_ldts, src_rsrc, historized, snapshot_trigger_column='is_active', snapshot_relation=none) -%}
{{ log('Test ob Ref Table Macro Call geht', info=True) }}
    {% set query %}
        INSERT INTO {{this.database}}.dbt_thoffmannsf_datavault4dbt_premium_package.ref_table_metadata (
        Ref_Table_Name,
        Ref_Hub,
        ref_sats,
        historized,
        snapshot_relation,
        snapshot_trigger_column,
        src_ldts,
        src_rsrc,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Ref_Table_Name,
        CAST('{{ ref_hub }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Ref_Hub,
        CAST('{{ ref_satellites | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ref_sats,
        CAST('{{ historized }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS historized,
        CAST('{{ snapshot_relation }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS snapshot_relation,
        CAST('{{ snapshot_trigger_column }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS snapshot_trigger_column,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}