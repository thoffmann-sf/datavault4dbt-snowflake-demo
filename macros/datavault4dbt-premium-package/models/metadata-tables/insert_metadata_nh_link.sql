{%- macro insert_metadata_nh_link(link_hashkey, foreign_hashkeys, payload, source_models, src_ldts, src_rsrc, disable_hwm, source_is_single_batch, union_strategy) -%}
{%- set json_source_models = tojson(source_models) -%}

{%- if not datavault4dbt.is_something(foreign_hashkeys) -%}
    {%- set foreign_hashkeys = [] -%}
{%- endif -%}

    {% set query %}
        INSERT INTO {{ get_model_db_name_dict('nh_link_metadata').full_name }} (
        Link_Name,
        Link_Hashkey,
        Foreign_Hashkeys,
        Payload,
        source_models,
        disable_hwm,
        source_is_single_batch,
        union_strategy,
        src_ldts,
        src_rsrc,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Link_Name,
        CAST('{{ link_hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Link_Hashkey,
        CAST('{{ foreign_hashkeys | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Foreign_Hashkeys,
        CAST('{{ payload | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Payload,
        CAST('{{ json_source_models }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_models,
        CAST('{{ disable_hwm }}' AS BOOLEAN) AS disable_hwm,
        CAST('{{ source_is_single_batch }}' AS BOOLEAN) AS source_is_single_batch,
        CAST('{{ union_strategy }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS union_strategy,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}