{%- macro insert_metadata_stage(include_source_columns,
                ldts,
                rsrc,
                source_model,
                hashed_columns,
                derived_columns,
                sequence,
                prejoined_columns,
                missing_columns,
                multi_active_config,
                enable_ghost_records) -%}
                
{%- set json_hashed_columns = tojson(hashed_columns) -%}
{%- set json_derived_columns = tojson(derived_columns) -%}
{%- set json_prejoined_columns = tojson(prejoined_columns) -%}

    {% set query %}
        INSERT INTO {{ get_model_db_name_dict('stage_metadata').full_name }} (
        stage_name,
        ldts,
        rsrc,
        source_model,
        include_source_columns,
        sequence,
        enable_ghost_records,
        hashed_columns,
        derived_columns,
        prejoin_columns,
        execution_timestamp,
        invocation_id
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS stage_name,
        CAST('{{ ldts | replace("'", "''") }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ldts,
        CAST('{{ rsrc | replace("'", "''") }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS rsrc,
        CAST('{{ source_model | replace("'", "''") }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS source_model,
        CAST('{{ include_source_columns }}' AS BOOLEAN) AS include_source_columns,
        CAST('{{ sequence }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS sequence,
        CAST('{{ enable_ghost_records }}' AS BOOLEAN) AS enable_ghost_records,
        CAST('{{ json_hashed_columns | replace("'", "''") }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS hashed_columnss,
        CAST('{{ json_derived_columns | replace("'", "''") }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS derived_columns,
        CAST('{{ json_prejoined_columns | replace("'", "''") }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS prejoin_columns,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}