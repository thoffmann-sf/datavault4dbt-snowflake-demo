{% set latest_id = get_latest_invocation_id('stage_metadata') %}

SELECT DISTINCT
    hub.hub_name,
    hub.hub_hashkey,
    eff_sat.sat_name AS eff_sat_name,
    eff_sat.tracked_hashkey
FROM {{ ref('hub_metadata') }} AS hub
LEFT JOIN {{ ref('eff_sat_metadata') }} AS eff_sat
    ON hub_hashkey = tracked_hashkey
WHERE eff_sat_name IS NULL
    AND hub.invocation_id = '{{ latest_id }}'
    AND eff_sat.invocation_id = '{{ latest_id }}'
