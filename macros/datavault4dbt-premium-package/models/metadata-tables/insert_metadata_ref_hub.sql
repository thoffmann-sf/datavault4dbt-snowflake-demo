{%- macro insert_metadata_ref_hub(ref_keys, src_ldts, src_rsrc, source_models) -%}
{%- set json_source_models = tojson(source_models) -%}
{{ log('Test ob Ref hub Macro Call geht', info=True) }}
    {% set query %}
        INSERT INTO {{this.database}}.dbt_thoffmannsf_datavault4dbt_premium_package.ref_hub_metadata (
        Hub_Name,
        ReF_Keys,
        source_models,
        src_ldts,
        src_rsrc,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Hub_Name,
        {% if ref_keys is iterable and ref_keys is not string %}
        CAST('{{ ref_keys | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ReF_Keys,
        {%- else %}
        CAST('{{ ref_keys }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ReF_Keys,
        {%- endif %}
        CAST('{{ json_source_models }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_models,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}