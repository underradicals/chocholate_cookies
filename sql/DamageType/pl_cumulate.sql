-- =================================================================================================
-- Here we use PL/SQL to check if the table `damage_type` exists on the bronze schema
-- if result is truthy:
--      We create the table for the first time with the data from the subquery
-- else
--      We run a merge on `damage_type` enforcing idempotency, row-level synchronization (RLS)
--      and atomicity.
-- =================================================================================================

DO
$$
    BEGIN
        IF NOT EXISTS(SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'bronze' and tablename = 'damage_type') THEN
            CREATE TABLE IF NOT EXISTS d2_data.bronze.damage_type AS
            SELECT (json ->> 'hash')::BIGINT                             damage_type_id,
                   (json -> 'displayProperties' ->> 'name')::TEXT        damage_type_name,
                   (json -> 'displayProperties' ->> 'description')::TEXT damage_type_description,
                   (json -> 'displayProperties' ->> 'icon')::TEXT        damage_type_url,
                   (json -> 'color' ->> 'red')::INT                      red,
                   (json -> 'color' ->> 'green')::INT                    green,
                   (json -> 'color' ->> 'blue')::INT                     blue,
                   (json -> 'color' ->> 'alpha')::INT                    alpha
            FROM d2_data.datalake.destiny_damage_type_definition
            WHERE (json -> 'displayProperties' ->> 'icon')::TEXT IS NOT NULL;
        ELSE
            MERGE INTO d2_data.bronze.damage_type bdt
            USING d2_data.datalake.destiny_damage_type_definition ddtd
            ON bdt.damage_type_id = (ddtd.json -> 'hash')::BIGINT
            WHEN MATCHED AND bdt.damage_type_url IS NOT NULL THEN
                UPDATE
                SET damage_type_id          = (ddtd.json ->> 'hash')::BIGINT,
                    damage_type_description = (ddtd.json -> 'displayProperties' ->> 'description')::TEXT,
                    damage_type_name        = (ddtd.json -> 'displayProperties' ->> 'name')::TEXT,
                    damage_type_url         = (ddtd.json -> 'displayProperties' ->> 'icon')::TEXT,
                    red                     = (ddtd.json -> 'color' ->> 'red')::INT,
                    green                   = (ddtd.json -> 'color' ->> 'green')::INT,
                    blue                    = (ddtd.json -> 'color' ->> 'blue')::INT,
                    alpha                   = (ddtd.json -> 'color' ->> 'alpha')::INT
            WHEN NOT MATCHED AND (ddtd.json -> 'displayProperties' ->> 'icon')::TEXT IS NOT NULL THEN
                INSERT (damage_type_id, damage_type_name, damage_type_description, damage_type_url, red, green, blue,
                        alpha)
                VALUES ((ddtd.json ->> 'hash')::BIGINT,
                        (ddtd.json -> 'displayProperties' ->> 'description')::TEXT,
                        (ddtd.json -> 'displayProperties' ->> 'name')::TEXT,
                        (ddtd.json -> 'displayProperties' ->> 'icon')::TEXT,
                        (ddtd.json -> 'color' ->> 'red')::INT,
                        (ddtd.json -> 'color' ->> 'green')::INT,
                        (ddtd.json -> 'color' ->> 'blue')::INT,
                        (ddtd.json -> 'color' ->> 'alpha')::INT);
        END IF;
    END;
$$;