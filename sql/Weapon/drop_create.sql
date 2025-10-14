create table if not exists d2_data.bronze.weapon AS
WITH cte AS (SELECT (json ->> 'hash')::BIGINT                                      AS weapon_id,
                    (json -> 'displayProperties' ->> 'name')::TEXT                 AS weapon_name,
                    split_part((json -> 'traitIds')::jsonb ->> (jsonb_array_length((json -> 'traitIds')::jsonb) - 1), '.', 2) as version,
                    split_part((json -> 'traitIds')::jsonb ->> (jsonb_array_length((json -> 'traitIds')::jsonb) - 1), '.', 3) as version_type,
                    (json ->> 'itemTypeDisplayName')::TEXT                            weapon_display_name,
                    (json -> 'inventory' ->> 'tierTypeName')::TEXT                 AS weapon_tier_type_name,
                    (CASE (json -> 'equippingBlock' ->> 'ammoType')::INT
                         WHEN 1 THEN 'Primary'
                         WHEN 2 THEN 'Special'
                         WHEN 3 THEN 'Power'
                        END)::TEXT                                                 AS weapon_ammo_type,
                    (json -> 'equippingBlock' ->> 'equipmentSlotTypeHash')::BIGINT AS equipment_slot_id,
                    (json ->> 'defaultDamageTypeHash')::BIGINT                     AS weapon_damage_type_id,
                    (json ->> 'flavorText')::TEXT                                     weaopon_flavor_text,
                    (json ->> 'screenshot')::TEXT                                  AS weapon_screenshot_url,
                    (json -> 'displayProperties' ->> 'icon')::TEXT                 AS weapon_icon_url,
                    (json ->> 'iconWatermark')::TEXT                               AS weapon_watermark_url
             FROM d2_data.datalake.destiny_inventory_item_definition
             WHERE (json ->> 'itemType')::INT = 3),
     dte AS (SELECT (json ->> 'hash')::BIGINT                             AS damage_type_id,
                    (json -> 'displayProperties' ->> 'name')::TEXT        AS damage_type_name,
                    (json -> 'displayProperties' ->> 'description')::TEXT AS damage_type_description,
                    (json ->> 'transparentIconPath')::TEXT                AS damage_type_transparent_icon_url
             FROM d2_data.datalake.destiny_damage_type_definition),
     ete AS (SELECT (json ->> 'hash')::BIGINT                             AS equipment_slot_id,
                    (json -> 'displayProperties' ->> 'name')::TEXT        AS equipment_slot_name,
                    (json -> 'displayProperties' ->> 'description')::TEXT AS equipment_slot_description
             FROM d2_data.datalake.destiny_equipment_slot_definition)
SELECT c.weapon_id,
       c.weapon_name,
       c.version,
       c.version_type,
       c.weapon_display_name,
       d.damage_type_name,
       e.equipment_slot_name,
       e.equipment_slot_description,
       d.damage_type_description,
       c.weapon_tier_type_name,
       c.weapon_ammo_type,
       c.weaopon_flavor_text,
       c.weapon_screenshot_url,
       c.weapon_icon_url,
       d.damage_type_transparent_icon_url
FROM cte c
         INNER JOIN dte d ON c.weapon_damage_type_id = d.damage_type_id
         INNER JOIN ete e ON c.equipment_slot_id = e.equipment_slot_id;