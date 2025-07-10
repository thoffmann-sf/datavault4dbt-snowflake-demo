{%- macro insert_metadata_ref_sat_v0(parent_ref_keys, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model, disable_hwm, source_is_single_batch) -%}
    {% set query %}
        INSERT INTO {{ get_model_db_name_dict('ref_sat_v0_metadata').full_name }} (
        Sat_Name,
        Parent_Refkey,
        Hashdiff,
        Payload,
        source_model,
        src_ldts,
        src_rsrc,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_Name,
        {% if parent_ref_keys is iterable and parent_ref_keys is not string %}
        CAST('{{ parent_ref_keys | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Parent_Refkey,
        {%- else %}
        CAST('{{ parent_ref_keys }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Parent_Refkey,
        {%- endif %}
        CAST('{{ src_hashdiff }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Hashdiff,
        CAST('{{ src_payload | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Payload,
        CAST('{{ source_model }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_model,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}