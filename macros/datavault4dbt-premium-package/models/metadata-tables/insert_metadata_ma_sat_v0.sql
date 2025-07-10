{%- macro insert_metadata_ma_sat_v0(parent_hashkey, src_hashdiff, src_ma_key, src_payload, src_ldts, src_rsrc, source_model) -%}
    {% set query %}
        INSERT INTO {{ get_model_db_name_dict('ma_sat_v0_metadata').full_name }} (
            Sat_Name,
            Parent_Hashkey,
            Hashdiff,
            Payload,
            MA_keys,
            source_model,
            src_ldts,
            src_rsrc,
            execution_timestamp,
            invocation_id
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_Name,
        CAST('{{ parent_hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Parent_Hashkey,
        CAST('{{ src_hashdiff }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Hashdiff,
        CAST('{{ src_payload | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Payload,
        {% if src_ma_key is iterable and src_ma_key is not string %}
        CAST('{{ src_ma_key | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS MA_keys,
        {%- else %}
        CAST('{{ src_ma_key }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS MA_keys,
        {%- endif %}
        CAST('{{ source_model }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_model,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}