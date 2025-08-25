{% test hubs_without_sats(model) %}

SELECT DISTINCT
    hub.hub_name,
    hub.hub_hashkey,
    sat.sat_name,
    sat.parent_hashkey
FROM {{ ref('hub_metadata') }} AS hub
LEFT JOIN {{ ref('sat_v0_metadata') }} AS sat
    ON hub.hub_hashkey = sat.parent_hashkey
WHERE sat.sat_name IS NULL
AND hub.invocation_id = {{ get_latest_invocation_id('hub_metadata') }}
AND sat.invocation_id = {{ get_latest_invocation_id('sat_v0_metadata') }}

{% endtest %}