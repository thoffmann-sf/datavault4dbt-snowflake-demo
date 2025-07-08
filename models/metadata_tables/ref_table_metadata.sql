/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (select 1 as foo)

select
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Ref_Table_Name,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Ref_Hub,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as ref_sats,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as historized,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as snapshot_relation,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as snapshot_trigger_column,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as src_ldts,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as src_rsrc,
    cast(null as {{ datavault4dbt.timestamp_default_dtype() }}) as execution_timestamp,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as invocation_id 
from dummy_cte
where 1 = 0
