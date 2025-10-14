DROP TABLE IF EXISTS d2_data.bronze.stat_type CASCADE;

CREATE TABLE IF NOT EXISTS d2_data.bronze.stat_type AS
SELECT (json ->> 'hash')::BIGINT                                                       AS stat_type_id,
       (json ->> 'index')::INT                                                         AS stat_type_index,
       (json ->> 'redacted')::BOOLEAN                                                  AS redacted,
       (json ->> 'blacklisted')::BOOLEAN                                               AS blacklisted,
       (json ->> 'interpolate')::BOOLEAN                                               AS interpolate,
       (json ->> 'statCategory')::INT                                                  AS stat_category,
       (json ->> 'aggregationType')::INT                                               AS aggregation_type,
       (json ->> 'hasComputedBlock')::BOOLEAN                                          AS has_computed_block,
       (json -> 'displayProperties' ->> 'name')::TEXT                                  AS stat_type_name,
       (json -> 'displayProperties' ->> 'hasIcon')::BOOLEAN                            AS has_icon,
       (json -> 'displayProperties' ->> 'iconHash')::INT                               AS stat_type_icon_hash,
       (json -> 'displayProperties' ->> 'description')::TEXT                           AS description,
       COALESCE((json -> 'displayProperties' ->> 'icon')::TEXT,
                '/common/destiny2_content/icons/435daeef2fc277af6476f2ffb9b18bcb.png') AS stat_type_icon
FROM d2_data.datalake.destiny_stat_definition
WHERE (json -> 'displayProperties' ->> 'name')::TEXT != '';