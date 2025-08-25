-- tests/generic/hub_eff_sat_existency.sql

{% test hub_eff_sat_existency(model) %}



SELECT DISTINCT
    hub.hub_name,
    hub.hub_hashkey,
    eff_sat.sat_name AS eff_sat_name,
    eff_sat.tracked_hashkey
FROM {{ ref('hub_metadata') }} AS hub
LEFT JOIN {{ ref('eff_sat_metadata') }} AS eff_sat
    ON hub.hub_hashkey = eff_sat.tracked_hashkey
WHERE eff_sat.sat_name IS NULL
    AND hub.invocation_id = {{ get_latest_invocation_id('hub_metadata') }}
    AND eff_sat.invocation_id = {{ get_latest_invocation_id('eff_sat_metadata') }}
{% endtest %}
