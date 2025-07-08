{%- macro insert_metadata_link(link_hashkey, foreign_hashkeys, source_models, src_ldts, src_rsrc, disable_hwm) -%}
{%- set json_source_models = tojson(source_models) -%}

{%- if not datavault4dbt.is_something(disable_hwm) -%}
    {%- set disable_hwm = 'FALSE' -%}
{%- endif -%}

{{ log('Test ob Link Macro Call geht', info=True) }}
    {% set query %}
        INSERT INTO {{this.database}}.dbt_thoffmannsf_datavault4dbt_premium_package.link_metadata (
            Link_Name,
            Link_Hashkey,
            Foreign_Hashkeys,
            source_models,
            disable_hwm,
            src_ldts,
            src_rsrc,
            execution_timestamp,
            invocation_id
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Link_Name,
        CAST('{{ link_hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Link_Hashkey,
        CAST('{{ foreign_hashkeys | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Foreign_Hashkeys,
        CAST('{{ json_source_models }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_models,
        CAST('{{ disable_hwm }}' AS BOOLEAN) AS disable_hwm,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}