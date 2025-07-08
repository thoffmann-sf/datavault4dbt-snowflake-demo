/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (select 1 as foo)

select
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Pit_Name,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Tracked_Hub,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Hub_Hashkey,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Sat_Names,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Snapshot_relation,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as dimension_key,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as pit_type,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as snapshot_trigger_column,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as ldts,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as custom_rsrc,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as ledts,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as sdts,
    cast(null as {{ datavault4dbt.timestamp_default_dtype() }}) as execution_timestamp,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as invocation_id 
from dummy_cte
where 1 = 0
