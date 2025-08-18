 

 SELECT DISTINCT
    link.link_name,
    link.link_hashkey,
    eff_sat.sat_name AS eff_sat_name,
    eff_sat.tracked_hashkey 
FROM {{ ref('link_metadata') }} AS link
LEFT JOIN {{ ref('eff_sat_metadata') }} AS eff_sat
ON link_hashkey=tracked_hashkey
WHERE eff_sat_name IS NULL 
AND tracked_hashkey IS NULL 









