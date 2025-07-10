{%- macro insert_metadata_ma_sat_v1(sat_v0, hashkey, hashdiff, ma_attribute, src_ldts, src_rsrc, ledts_alias, add_is_current_flag) -%}
    {% set query %}
        INSERT INTO {{ get_model_db_name_dict('ma_sat_v1_metadata').full_name }} (
            Sat_Name,
            Sat_v0,
            Parent_Hashkey,
            Hashdiff,
            MA_keys,
            src_ldts,
            src_rsrc,
            ledts_alias,
            add_is_current_flag,
            execution_timestamp,
            invocation_id
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_Name,
        CAST('{{ sat_v0 }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_v0,
        CAST('{{ hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Parent_Hashkey,
        CAST('{{ hashdiff }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Hashdiff,
        {% if ma_attribute is iterable and ma_attribute is not string %}
        CAST('{{ ma_attribute | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS MA_keys,
        {%- else %}
        CAST('{{ ma_attribute }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS MA_keys,
        {%- endif %}
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST('{{ ledts_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ledts_alias,
        CAST('{{ add_is_current_flag }}' AS BOOLEAN) AS add_is_current_flag,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}