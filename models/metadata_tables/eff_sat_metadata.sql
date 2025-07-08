/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (select 1 as foo)

select
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Sat_Name,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Tracked_Hashkey,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as source_model,
    cast(null as boolean) as disable_hwm,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as src_ldts,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as src_rsrc,
    cast(null as boolean) as source_is_single_batch,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as is_active_alias,
    cast(null as {{ datavault4dbt.timestamp_default_dtype() }}) as execution_timestamp,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as invocation_id 
from dummy_cte
where 1 = 0
