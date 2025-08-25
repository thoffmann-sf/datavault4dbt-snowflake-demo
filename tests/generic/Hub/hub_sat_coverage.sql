{% test hub_sat_coverage(model) %}

WITH hub_counts AS (
    SELECT
        COUNT(DISTINCT hub_name) AS total_hubs
    FROM {{ ref('hub_metadata') }}
    where invocation_id = {{ get_latest_invocation_id('hub_metadata') }}
),

satellite_counts AS (
    SELECT
        COUNT(DISTINCT sat_name) AS total_satellites
    FROM {{ ref('sat_v0_metadata') }}
    where invocation_id = {{ get_latest_invocation_id('sat_v0_metadata') }}
),

validation_errors AS (
    SELECT
        hub_counts.total_hubs,
        satellite_counts.total_satellites
    FROM hub_counts
    CROSS JOIN satellite_counts
    WHERE satellite_counts.total_satellites < hub_counts.total_hubs
)

SELECT * FROM validation_errors

{% endtest %}