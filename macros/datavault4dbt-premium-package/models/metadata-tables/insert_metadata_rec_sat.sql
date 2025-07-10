{%- macro insert_metadata_rec_sat(tracked_hashkey, source_models, src_ldts, src_rsrc, src_stg, disable_hwm) -%}
{%- set json_source_models = tojson(source_models) -%}
    {% set query %}
        INSERT INTO {{ get_model_db_name_dict('rec_sat_metadata').full_name }} (
        Sat_Name,
        Tracked_Hashkey,
        source_models,
        disable_hwm,
        src_ldts,
        src_rsrc,
        src_stg,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_Name,
        CAST('{{ tracked_hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Tracked_Hashkey,
        CAST('{{ json_source_models }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_models,
        CAST('{{ disable_hwm }}' AS BOOLEAN) AS disable_hwm,
        CAST('{{ src_ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_ldts,
        CAST('{{ src_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_rsrc,
        CAST('{{ src_stg }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS src_stg,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}