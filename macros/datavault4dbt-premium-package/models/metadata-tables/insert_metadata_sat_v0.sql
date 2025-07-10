{%- macro insert_metadata_sat_v0(parent_hashkey, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model, disable_hwm, source_is_single_batch) -%}
    {% set query %}
        INSERT INTO {{ get_model_db_name_dict('sat_v0_metadata').full_name }} (
        Sat_Name,
        Parent_Hashkey,
        hashdiff,
        Payload,
        source_model,
        src_ldts,
        src_rsrc,
        disable_hwm,
        source_is_single_batch,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_Name,
        CAST('{{ parent_hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Parent_Hashkey,
        CAST('{{ src_hashdiff }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS hashdiff,
        CAST('{{ src_payload | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Payload,
        CAST('{{ source_model }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_model,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST('{{ disable_hwm }}' AS boolean) AS disable_hwm,
        CAST('{{ source_is_single_batch }}' AS boolean) AS source_is_single_batch,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}