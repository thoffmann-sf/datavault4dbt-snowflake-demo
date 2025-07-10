{%- macro insert_metadata_pit(tracked_entity, hashkey, sat_names, ldts, ledts, sdts, snapshot_relation, dimension_key, refer_to_ghost_records, snapshot_trigger_column=none, custom_rsrc=none, pit_type=none) -%}
    {% set query %}
        INSERT INTO {{ get_model_db_name_dict('pit_metadata').full_name }} (
        Pit_Name,
        Tracked_Hub,
        Hub_Hashkey,
        Sat_Names,
        Snapshot_relation,
        dimension_key,
        pit_type,
        snapshot_trigger_column,
        ldts,
        custom_rsrc,
        ledts,
        sdts,
        execution_timestamp,
        invocation_id 
        )
    SELECT
        CAST('{{ this.table }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Pit_Name,
        CAST('{{ tracked_entity }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Tracked_Hub,
        CAST('{{ hashkey }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Hub_Hashkey,
        CAST('{{ sat_names | join(', ') }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Sat_Names,
        CAST('{{ snapshot_relation }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS Snapshot_relation,
        CAST('{{ dimension_key }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS dimension_key,
        CAST('{{ pit_type }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS pit_type,
        CAST('{{ snapshot_trigger_column }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS snapshot_trigger_column,
        CAST('{{ ldts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ldts,
        CAST('{{ custom_rsrc }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS custom_rsrc,
        CAST('{{ ledts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS ledts,
        CAST('{{ sdts }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS sdts,
        CAST(CURRENT_TIMESTAMP AS {{ datavault4dbt.timestamp_default_dtype() }}) AS execution_timestamp,
        CAST('{{ invocation_id }}' AS {{ datavault4dbt.string_default_dtype(type=none) }}) AS invocation_id
    {% endset %}
    {% do run_query(query) %}
{%- endmacro -%}