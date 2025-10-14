-- =================================================================================================
-- Rebuild d2_data.datalake
-- =================================================================================================

DO
$$
    DECLARE
        base_path    text   := 'Z:\data\csv\';
        file_names   text[] := array [
            'DestinyAchievementDefinition',
            'DestinyActivityDefinition',
            'DestinyActivityDifficultyTierCollectionDefinition',
            'DestinyActivityFamilyDefinition',
            'DestinyActivityGraphDefinition',
            'DestinyActivityInteractableDefinition',
            'DestinyActivityLoadoutRestrictionDefinition',
            'DestinyActivityModeDefinition',
            'DestinyActivityModifierDefinition',
            'DestinyActivitySelectableSkullCollectionDefinition',
            'DestinyActivitySelectableSkullExclusionGroupDefinition',
            'DestinyActivitySkullCategoryDefinition',
            'DestinyActivitySkullCollectionDefinition',
            'DestinyActivitySkullSubcategoryDefinition',
            'DestinyActivityTypeDefinition',
            'DestinyArtDyeChannelDefinition',
            'DestinyArtDyeReferenceDefinition',
            'DestinyArtifactDefinition',
            'DestinyBondDefinition',
            'DestinyBreakerTypeDefinition',
            'DestinyCharacterCustomizationCategoryDefinition',
            'DestinyCharacterCustomizationOptionDefinition',
            'DestinyChecklistDefinition',
            'DestinyClassDefinition',
            'DestinyCollectibleDefinition',
            'DestinyDamageTypeDefinition',
            'DestinyDestinationDefinition',
            'DestinyEnergyTypeDefinition',
            'DestinyEntitlementOfferDefinition',
            'DestinyEquipableItemSetDefinition',
            'DestinyEquipmentSlotDefinition',
            'DestinyEventCardDefinition',
            'DestinyFactionDefinition',
            'DestinyFireteamFinderActivityGraphDefinition',
            'DestinyFireteamFinderActivitySetDefinition',
            'DestinyFireteamFinderConstantsDefinition',
            'DestinyFireteamFinderLabelDefinition',
            'DestinyFireteamFinderLabelGroupDefinition',
            'DestinyFireteamFinderOptionDefinition',
            'DestinyFireteamFinderOptionGroupDefinition',
            'DestinyGenderDefinition',
            'DestinyGlobalConstantsDefinition',
            'DestinyGuardianRankConstantsDefinition',
            'DestinyGuardianRankDefinition',
            'DestinyIconDefinition',
            'DestinyInventoryBucketDefinition',
            'DestinyInventoryItemConstantsDefinition',
            'DestinyInventoryItemDefinition',
            'DestinyInventoryItemLiteDefinition',
            'DestinyItemCategoryDefinition',
            'DestinyItemFilterDefinition',
            'DestinyItemTierTypeDefinition',
            'DestinyLoadoutColorDefinition',
            'DestinyLoadoutConstantsDefinition',
            'DestinyLoadoutIconDefinition',
            'DestinyLoadoutNameDefinition',
            'DestinyLocationDefinition',
            'DestinyLoreDefinition',
            'DestinyMaterialRequirementSetDefinition',
            'DestinyMedalTierDefinition',
            'DestinyMetricDefinition',
            'DestinyMilestoneDefinition',
            'DestinyObjectiveDefinition',
            'DestinyPlaceDefinition',
            'DestinyPlatformBucketMappingDefinition',
            'DestinyPlugSetDefinition',
            'DestinyPowerCapDefinition',
            'DestinyPresentationNodeDefinition',
            'DestinyProgressionDefinition',
            'DestinyProgressionLevelRequirementDefinition',
            'DestinyProgressionMappingDefinition',
            'DestinyRaceDefinition',
            'DestinyRecordDefinition',
            'DestinyReportReasonCategoryDefinition',
            'DestinyRewardAdjusterPointerDefinition',
            'DestinyRewardAdjusterProgressionMapDefinition',
            'DestinyRewardItemListDefinition',
            'DestinyRewardMappingDefinition',
            'DestinyRewardSourceDefinition',
            'DestinySackRewardItemListDefinition',
            'DestinySandboxPatternDefinition',
            'DestinySandboxPerkDefinition',
            'DestinySeasonDefinition',
            'DestinySeasonPassDefinition',
            'DestinySocialCommendationDefinition',
            'DestinySocialCommendationNodeDefinition',
            'DestinySocketCategoryDefinition',
            'DestinySocketTypeDefinition',
            'DestinyStatDefinition',
            'DestinyStatGroupDefinition',
            'DestinyTraitDefinition',
            'DestinyUnlockCountMappingDefinition',
            'DestinyUnlockDefinition',
            'DestinyUnlockEventDefinition',
            'DestinyUnlockExpressionMappingDefinition',
            'DestinyUnlockValueDefinition',
            'DestinyVendorDefinition',
            'DestinyVendorGroupDefinition'
            ];
        item         text;
        drop_sql     text;
        create_sql   text;
        populate_sql text;
    BEGIN
        FOREACH item IN ARRAY file_names
            LOOP
                drop_sql := format('drop table if exists d2_data.datalake.%I cascade', d2_data.datalake.to_snake_case(item));
                create_sql := format('create table if not exists d2_data.datalake.%I (id bigint primary key, json jsonb)',
                                     d2_data.datalake.to_snake_case(item));
                populate_sql :=
                        format('copy d2_data.datalake.%I from %L with (format csv, header true)', d2_data.datalake.to_snake_case(item),
                               base_path || item || '.csv');
                RAISE NOTICE 'Executing: %', drop_sql;
                RAISE NOTICE 'Executing: %', create_sql;
                RAISE NOTICE 'Executing: %', populate_sql;
                EXECUTE drop_sql;
                EXECUTE create_sql;
                EXECUTE populate_sql;
            end loop;
    END
$$;