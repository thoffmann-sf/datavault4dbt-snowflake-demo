{%- macro insert_metadata_ref_sat_v1(ref_sat_v0, ref_keys, hashdiff, src_ldts, src_rsrc, ledts_alias, add_is_current_flag) -%}
{{ log('Test ob Ref Sat V1 Macro Call geht', info=True) }}
    {% set query %}
        INSERT INTO {{this.database}}.dbt_thoffmannsf_datavault4dbt_premium_package.ref_sat_v1_metadata (
        Sat_Name,
        Parent_Refkey,
        ref_satv0_name,
        Hashdiff,
        ledts_alias,
        src_ldts,
        src_rsrc,
        add_is_current_flag,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_Name,
        {% if ref_keys is iterable and ref_keys is not string %}
        CAST('{{ ref_keys | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Parent_Refkey,
        {%- else %}
        CAST('{{ ref_keys }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Parent_Refkey,
        {%- endif %}
        CAST('{{ ref_sat_v0 }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ref_satv0_name,
        CAST('{{ hashdiff }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Hashdiff,
        CAST('{{ ledts_alias }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ledts_alias,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST('{{ add_is_current_flag }}' AS boolean) AS add_is_current_flag,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}