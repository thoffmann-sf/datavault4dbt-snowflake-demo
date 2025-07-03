/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (select 1 as foo)

select
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Hub_Name,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Hub_Hashkey,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Business_keys,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as source_models,
    cast(null as boolean) as disable_hwm,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as src_ldts,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as src_rsrc,
    cast(null as {{ datavault4dbt.timestamp_default_dtype() }}) as execution_timestamp,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as invocation_id 
from dummy_cte
where 1 = 0
