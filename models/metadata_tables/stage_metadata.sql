/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (select 1 as foo)

select
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }})as stage_name,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }})as ldts,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }})as rsrc,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }})as source_model,
    cast(null as boolean) as include_source_columns,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }})as sequence,
    cast(null as boolean) as enable_ghost_records,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }})as hashed_columns,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }})as derived_columns,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }})as prejoin_columns,
    cast(null as {{ datavault4dbt.timestamp_default_dtype() }}) as execution_timestamp,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as invocation_id 
from dummy_cte
where 1 = 0