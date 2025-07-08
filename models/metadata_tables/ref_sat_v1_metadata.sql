/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (select 1 as foo)

select
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Sat_Name,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Parent_Refkey,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as ref_satv0_name,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as Hashdiff,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as ledts_alias,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as src_ldts,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as src_rsrc,
    cast(null as boolean) as add_is_current_flag,
    cast(null as {{ datavault4dbt.timestamp_default_dtype() }}) as execution_timestamp,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as invocation_id 
from dummy_cte
where 1 = 0
