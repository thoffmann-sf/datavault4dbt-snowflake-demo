 SELECT 
    hub.hub_name,
    hub.hub_hashkey,
    eff_sat.sat_name,
    eff_sat.tracked_hashkey 
FROM {{ ref('hub_metadata') }} AS hub
LEFT JOIN {{ ref('eff_sat_metadata') }} AS eff_sat
ON hub_hashkey=tracked_hashkey
WHERE ere sat_name IS NULL 
AND tracked_hashkey IS NULL 







