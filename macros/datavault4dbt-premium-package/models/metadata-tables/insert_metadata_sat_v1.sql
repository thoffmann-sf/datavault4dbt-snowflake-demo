{%- macro insert_metadata_sat_v1(sat_v0, hashkey, hashdiff, src_ldts, src_rsrc, ledts_alias, add_is_current_flag, include_payload) -%}
    {% set query %}
        INSERT INTO {{ get_model_db_name_dict('sat_v1_metadata').full_name }} (
        Sat_Name,
        sat_v0_name,
        hashkey,
        hashdiff,
        src_ldts,
        src_rsrc,
        ledts_alias,
        add_is_current_flag,
        include_payload,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_Name,
        CAST('{{ sat_v0 }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS sat_v0_name,
        CAST('{{ hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS hashkey,
        CAST('{{ hashdiff }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS hashdiff,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST('{{ ledts_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ledts_alias,
        CAST('{{ add_is_current_flag }}' AS boolean) AS add_is_current_flag,
        CAST('{{ include_payload }}' AS boolean) AS include_payload,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}