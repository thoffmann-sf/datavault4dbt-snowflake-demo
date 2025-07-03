{%- macro insert_metadata_hub(hashkey, business_keys, src_ldts, src_rsrc, source_models, disable_hwm) -%}
{%- set json_source_models = tojson(source_models) -%}
{{ log('Test ob Macro Call geht', info=True) }}
    {% set query %}
        INSERT INTO {{this.database}}.dbt_thoffmannsf_datavault4dbt_premium_package.hub_metadata (
            Hub_Name,
            Hub_Hashkey,
            Business_keys,
            source_models,
            disable_hwm,
            src_ldts,
            src_rsrc,
            execution_timestamp,
            invocation_id
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Hub_Name,
        CAST('{{ hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Hub_Hashkey,
        CAST('{{ business_keys }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Business_keys,
        CAST('{{ json_source_models }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_models,
        CAST('{{ disable_hwm }}' AS BOOLEAN) AS disable_hwm,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}