SELECT
  'v0 Satellite ' || v0_sats.sat_name || ' does not have a corresponding v1 Satellite.' AS error_message,
  v0_sats.sat_name AS v0_satellite_name
FROM user_spaces.user_tderksen.sat_v0_metadata AS v0_sats
LEFT JOIN user_spaces.user_tderksen.sat_v1_metadata AS v1_sats
  ON v0_sats.sat_name = v1_sats.sat_v0_name
WHERE v1_sats.sat_v0_name IS NULL