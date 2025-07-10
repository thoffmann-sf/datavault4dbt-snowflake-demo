/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (select 1 as foo)

select
    cast(null as {{ datavault4dbt.timestamp_default_dtype() }}) as execution_timestamp,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as ldts_alias,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as rsrc_alias,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as ledts_alias,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as sdts_alias,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as snapshot_trigger_column,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as stg_alias,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as is_current_col_alias,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as is_active_alias,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as beginning_of_all_times,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as end_of_all_times,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as timestamp_format,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as beginning_of_all_times_date,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as end_of_all_times_date,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as date_format,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as default_unknow_rsrc,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as default_error_rsrc,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as rsrc_default_dtype,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as stg_default_dtype,
    cast(null as {{ datavault4dbt.string_default_dtype() }}) as derived_columns_default_dtype,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as hash,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as hash_datatype,
    cast(null as boolean) as hashkey_input_case_sensitivity,
    cast(null as boolean) as hashdiff_input_case_sensitivity,
    cast(null as boolean) as copy_rsrs_ldts_input_columns,
    cast(null as {{ datavault4dbt.string_default_dtype(type=none) }}) as invocation_id 
from dummy_cte
where 1 = 0