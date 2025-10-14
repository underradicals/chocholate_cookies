-- =================================================================================================
-- Merge `damage_type` with `damage_type_definition`
-- Enforces idempotency, row-level synchronization (RLS) and atomicity.
-- =================================================================================================

merge into d2_data.bronze.damage_type bdt
using d2_data.datalake.destiny_damage_type_definition ddtd
on bdt.damage_type_id = (ddtd.json -> 'hash')::bigint
when matched and bdt.damage_type_url is not null then
    update
    set damage_type_id          = (ddtd.json ->> 'hash')::bigint,
        damage_type_description = (ddtd.json -> 'displayProperties' ->> 'description')::text,
        damage_type_name        = (ddtd.json -> 'displayProperties' ->> 'name')::text,
        damage_type_url         = (ddtd.json -> 'displayProperties' ->> 'icon')::text,
        red                     = (ddtd.json -> 'color' ->> 'red')::int,
        green                   = (ddtd.json -> 'color' ->> 'green')::int,
        blue                    = (ddtd.json -> 'color' ->> 'blue')::int,
        alpha                   = (ddtd.json -> 'color' ->> 'alpha')::int
when not matched and (ddtd.json -> 'displayProperties' ->> 'icon')::text is not null then
    insert (damage_type_id, damage_type_name, damage_type_description, damage_type_url, red, green, blue, alpha)
    VALUES ((ddtd.json ->> 'hash')::bigint,
            (ddtd.json -> 'displayProperties' ->> 'description')::text,
            (ddtd.json -> 'displayProperties' ->> 'name')::text,
            (ddtd.json -> 'displayProperties' ->> 'icon')::text,
            (ddtd.json -> 'color' ->> 'red')::int,
            (ddtd.json -> 'color' ->> 'green')::int,
            (ddtd.json -> 'color' ->> 'blue')::int,
            (ddtd.json -> 'color' ->> 'alpha')::int);