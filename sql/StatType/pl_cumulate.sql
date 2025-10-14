DO
$$
    BEGIN
        IF NOT EXISTS(SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'bronze' and tablename = 'stat_type') THEN
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
        ELSE
            MERGE INTO d2_data.bronze.stat_type AS bst
            USING d2_data.datalake.destiny_stat_definition AS ddsd
            ON bst.stat_type_id = (ddsd.json ->> 'hash')::BIGINT
            WHEN MATCHED THEN
                UPDATE
                SET stat_type_id        = (ddsd.json ->> 'hash')::BIGINT,
                    stat_type_index     = (ddsd.json ->> 'index')::INT,
                    redacted            = (ddsd.json ->> 'redacted')::BOOLEAN,
                    blacklisted         = (ddsd.json ->> 'blacklisted')::BOOLEAN,
                    interpolate         = (ddsd.json ->> 'interpolate')::BOOLEAN,
                    stat_category       = (ddsd.json ->> 'statCategory')::INT,
                    aggregation_type    = (ddsd.json ->> 'aggregationType')::INT,
                    has_computed_block  = (ddsd.json ->> 'hasComputedBlock')::BOOLEAN,
                    stat_type_name      = (ddsd.json -> 'displayProperties' ->> 'name')::TEXT,
                    has_icon            = (ddsd.json -> 'displayProperties' ->> 'hasIcon')::BOOLEAN,
                    stat_type_icon_hash = (ddsd.json -> 'displayProperties' ->> 'iconHash')::INT,
                    description         = (ddsd.json -> 'displayProperties' ->> 'description')::TEXT,
                    stat_type_icon      = COALESCE((ddsd.json -> 'displayProperties' ->> 'icon')::TEXT,
                                                   '/common/destiny2_content/icons/435daeef2fc277af6476f2ffb9b18bcb.png')
            WHEN NOT MATCHED THEN
                INSERT (stat_type_id, stat_type_index, redacted, blacklisted, interpolate, stat_category,
                        aggregation_type,
                        has_computed_block, stat_type_name, has_icon, stat_type_icon_hash, description, stat_type_icon)
                VALUES ((ddsd.json ->> 'hash')::BIGINT,
                        (ddsd.json ->> 'index')::INT,
                        (ddsd.json ->> 'redacted')::BOOLEAN,
                        (ddsd.json ->> 'blacklisted')::BOOLEAN,
                        (ddsd.json ->> 'interpolate')::BOOLEAN,
                        (ddsd.json ->> 'statCategory')::INT,
                        (ddsd.json ->> 'aggregationType')::INT,
                        (ddsd.json ->> 'hasComputedBlock')::BOOLEAN,
                        (ddsd.json -> 'displayProperties' ->> 'name')::TEXT,
                        (ddsd.json -> 'displayProperties' ->> 'hasIcon')::BOOLEAN,
                        (ddsd.json -> 'displayProperties' ->> 'iconHash')::INT,
                        (ddsd.json -> 'displayProperties' ->> 'description')::TEXT,
                        COALESCE((ddsd.json -> 'displayProperties' ->> 'icon')::TEXT,
                                 '/common/destiny2_content/icons/435daeef2fc277af6476f2ffb9b18bcb.png'));
        END IF;
    END;
$$;