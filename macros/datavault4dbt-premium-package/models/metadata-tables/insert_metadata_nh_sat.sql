{%- macro insert_metadata_nh_sat(parent_hashkey, src_payload, src_ldts, src_rsrc, source_model, source_is_single_batch) -%}

{{ log('Test ob NH Sat Macro Call geht', info=True) }}
    {% set query %}
        INSERT INTO {{this.database}}.dbt_thoffmannsf_datavault4dbt_premium_package.nh_sat_metadata (
        Sat_Name,
        Parent_Hashkey,
        Payload,
        source_model,
        source_is_single_batch,
        src_ldts,
        src_rsrc,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_Name,
        CAST('{{ parent_hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Parent_Hashkey,
        CAST('{{ src_payload | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Payload,
        CAST('{{ source_model }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_model,
        CAST('{{ source_is_single_batch }}' AS BOOLEAN) AS source_is_single_batch,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}