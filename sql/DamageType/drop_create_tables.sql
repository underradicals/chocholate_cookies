-- =================================================================================================
-- Drop Table `damage_type` from database d2_data on schema bronze
-- =================================================================================================

drop table if exists d2_data.bronze.damage_type cascade;

-- =================================================================================================
-- Create table `damage_type` from database d2_data on schema bronze
-- =================================================================================================

create table if not exists d2_data.bronze.damage_type as
select (json ->> 'hash')::bigint                             damage_type_id,
       (json -> 'displayProperties' ->> 'name')::text        damage_type_name,
       (json -> 'displayProperties' ->> 'description')::text damage_type_description,
       (json -> 'displayProperties' ->> 'icon')::text        damage_type_url,
       (json -> 'color' ->> 'red')::int                      red,
       (json -> 'color' ->> 'green')::int                    green,
       (json -> 'color' ->> 'blue')::int                     blue,
       (json -> 'color' ->> 'alpha')::int                    alpha
from d2_data.datalake.destiny_damage_type_definition
where (json -> 'displayProperties' ->> 'icon')::text is not null;