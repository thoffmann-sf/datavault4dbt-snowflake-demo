/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (select 1 as foo)

select
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Snap_name,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as start_date,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as daily_snapshot_time,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as sdts_alias,
    cast(null as {{ datavault4dbt.timestamp_default_dtype() }}) as execution_timestamp,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as invocation_id 
from dummy_cte
where 1 = 0
