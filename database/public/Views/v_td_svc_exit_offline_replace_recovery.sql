DROP VIEW IF EXISTS v_td_svc_exit_offline_replace_recovery CASCADE;

CREATE OR REPLACE VIEW v_td_svc_exit_offline_replace_recovery
(
  message_type,
  payload_version_number,
  message_sequence_number,
  bss_equipment_id,
  length_of_message,
  transaction_id,
  txn_datetime,
  service_provider_id,
  unconfirmed_indicator,
  project_id,
  issuer_id,
  card_id,
  card_type,
  lifecycle,
  card_status,
  block_flag,
  test_flag,
  card_sequence_number,
  card_unblock_sequence_number,
  personalization_flag,
  purse_value,
  lav_service_provider_id,
  lav_location_code,
  lav_txn_datetime,
  lav_amount,
  lav_payment,
  lav_equipmentid,
  lav_csn,
  transaction_value,
  operating_day,
  equipment_no,
  fare,
  entry_location,
  entry_equipno,
  exit_location,
  fare_mode,
  array_number,
  fare_collection,
  entry_datetime,
  trans_mac_version,
  trans_mac_value,
  bts_ext_format_version_number,
  bts_ext_ar_seq_num,
  bts_ext_device_msg_seq_num,
  bts_ext_station_id,
  bts_ext_equipment_id,
  bts_ext_equipment_number,
  bts_ext_array_number,
  bts_ext_cloud_id_ar_seq,
  bts_ext_cloud_id_dev_msg_seq,
  bts_ext_spare,
  ssid,
  process_time,
  offline_replace_recovery,
  self
)
AS 
 SELECT 240 AS message_type,
    3 AS payload_version_number,
    0 AS message_sequence_number,
    pr."DeviceId" AS bss_equipment_id,
    100 AS length_of_message,
    153 AS transaction_id,
    tr."ExitTimestamp" AS txn_datetime,
    1 AS service_provider_id,
        CASE
            WHEN NOT pr."ValidatorUnconfirmedCommit" THEN 0
            ELSE 1
        END AS unconfirmed_indicator,
    1 AS project_id,
    "substring"(tk."CardId", 1, 3) AS issuer_id,
    COALESCE(
        CASE
            WHEN get_svc_exit_entry_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
            CASE
                WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
                WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
                WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
                   FROM "Presentations" p1
                  WHERE p1."Self" = tu."Href")
                ELSE '-1'::integer
            END, pr."LocationId") = 1 AND (get_svc_exit_exit_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
            CASE
                WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
                WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
                WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
                   FROM "Presentations" p1
                  WHERE p1."Self" = tu."Href")
                ELSE '-1'::integer
            END, pr."LocationId") = ANY (ARRAY[31, 32, 33, 34, 35, 53, 54, 55, 56, 57, 58, 59, 60, 61])) THEN
            CASE
                WHEN COALESCE(tr."PassUsedHref", COALESCE(tr."PassAtEntryHref", 'N'::text)) = 'N'::text THEN tk."CardId"
                ELSE COALESCE(( SELECT DISTINCT ( SELECT DISTINCT tk_1."CardId"
                           FROM "Tokens" tk_1
                          WHERE tk_1."State" = 'bound'::text AND tk_1."Self" = COALESCE(pa."IssuedAsTokenHref", pa."TransferredTokenHref")) AS "CardId"
                   FROM "Passes" pa
                  WHERE pa."TransferredDirection" = 'from'::text AND pa."Self" = COALESCE(tr."PassUsedHref", tr."PassAtEntryHref")), tk."CardId")
            END
            WHEN get_svc_exit_entry_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
            CASE
                WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
                WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
                WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
                   FROM "Presentations" p1
                  WHERE p1."Self" = tu."Href")
                ELSE '-1'::integer
            END, pr."LocationId") = 30 AND (get_svc_exit_exit_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
            CASE
                WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
                WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
                WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
                   FROM "Presentations" p1
                  WHERE p1."Self" = tu."Href")
                ELSE '-1'::integer
            END, pr."LocationId") = ANY (ARRAY[36, 37, 38, 39])) THEN
            CASE
                WHEN COALESCE(tr."PassUsedHref", COALESCE(tr."PassAtEntryHref", 'N'::text)) = 'N'::text THEN tk."CardId"
                ELSE COALESCE(( SELECT DISTINCT ( SELECT DISTINCT tk_1."CardId"
                           FROM "Tokens" tk_1
                          WHERE tk_1."State" = 'bound'::text AND tk_1."Self" = COALESCE(pa."IssuedAsTokenHref", pa."TransferredTokenHref")) AS "CardId"
                   FROM "Passes" pa
                  WHERE pa."TransferredDirection" = 'from'::text AND pa."Self" = COALESCE(tr."PassUsedHref", tr."PassAtEntryHref")), tk."CardId")
            END
            ELSE
            CASE
                WHEN COALESCE(tr."PassUsedHref", 'N'::text) = 'N'::text THEN tk."CardId"
                ELSE COALESCE(( SELECT DISTINCT ( SELECT DISTINCT tk_1."CardId"
                           FROM "Tokens" tk_1
                          WHERE tk_1."State" = 'bound'::text AND tk_1."Self" = COALESCE(pa."IssuedAsTokenHref", pa."TransferredTokenHref")) AS "CardId"
                   FROM "Passes" pa
                  WHERE pa."TransferredDirection" = 'from'::text AND pa."Self" = tr."PassUsedHref"), tk."CardId")
            END
        END, tk."CardId") AS card_id,
    tk."CardTypeId" AS card_type,
    tk."CardLifecycle"::integer AS lifecycle,
    0 AS card_status,
    0 AS block_flag,
    0 AS test_flag,
    0 AS card_sequence_number,
    0 AS card_unblock_sequence_number,
    0 AS personalization_flag,
    0 AS purse_value,
    0 AS lav_service_provider_id,
    0 AS lav_location_code,
    NULL::timestamp without time zone AS lav_txn_datetime,
    0 AS lav_amount,
    0 AS lav_payment,
    0 AS lav_equipmentid,
    0 AS lav_csn,
    COALESCE(tr."FareCharged"::numeric, 0.00) * 100::numeric AS transaction_value,
    pr."ServiceProviderOperatingDate" AS operating_day,
    pr."DeviceEquipmentName" AS equipment_no,
    COALESCE(tr."FareCharged"::numeric, 0.00) * 100::numeric AS fare,
    get_svc_exit_entry_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
        CASE
            WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
            WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
            WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
               FROM "Presentations" p1
              WHERE p1."Self" = tu."Href")
            ELSE '-1'::integer
        END, pr."LocationId") AS entry_location,
        CASE
            WHEN tr."EntryHref" IS NOT NULL AND pr_entry."DeviceEquipmentShortName" IS NOT NULL THEN "right"(pr_entry."DeviceEquipmentShortName", 2)
            WHEN tr."EntryHref" IS NOT NULL AND ofp."DeviceEquipmentShortName" IS NOT NULL THEN "right"(ofp."DeviceEquipmentShortName", 2)
            WHEN tu."Href" IS NOT NULL THEN ( SELECT "right"(p1."DeviceEquipmentShortName", 2) AS "right"
               FROM "Presentations" p1
              WHERE p1."Self" = tu."Href")
            ELSE NULL::text
        END AS entry_equipno,
    get_svc_exit_exit_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
        CASE
            WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
            WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
            WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
               FROM "Presentations" p1
              WHERE p1."Self" = tu."Href")
            ELSE '-1'::integer
        END, pr."LocationId") AS exit_location,
        CASE
            WHEN pr."ValidatorFareModeEntryExitOverride" = true THEN 1
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeTimeOverride" = true THEN 2
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeExcessFareOverride" = true THEN 4
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeFareFareBypass1" = true THEN 8
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeFareFareBypass2" = true THEN 16
            ELSE 0
        END AS fare_mode,
    pr."LocationArrayNumber" AS array_number,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) = 'N'::text AND COALESCE(tr."PassAtEntryHref", 'N'::text) = 'N'::text THEN 0::numeric
            ELSE
            CASE
                WHEN COALESCE(tr."PassUsedHref", 'N'::text) = 'N'::text THEN 0
                ELSE COALESCE(get_fare_collection(COALESCE(pr_entry."LocationId", ofp."LocationId"), pr."LocationId"), 0)
            END::numeric
        END AS fare_collection,
        CASE
            WHEN tr."EntryHref" IS NOT NULL THEN tr."EntryTimestamp"
            WHEN tu."Href" IS NOT NULL THEN tu."Timestamp"
            ELSE NULL::timestamp without time zone
        END AS entry_datetime,
    0 AS trans_mac_version,
    0 AS trans_mac_value,
    1 AS bts_ext_format_version_number,
    0 AS bts_ext_ar_seq_num,
    0 AS bts_ext_device_msg_seq_num,
    pr."LocationId" AS bts_ext_station_id,
    pr."DeviceEquipmentName" AS bts_ext_equipment_id,
    pr."DeviceEquipmentShortName" AS bts_ext_equipment_number,
    pr."LocationArrayNumber" AS bts_ext_array_number,
    0 AS bts_ext_cloud_id_ar_seq,
    0::bigint AS bts_ext_cloud_id_dev_msg_seq,
    0 AS bts_ext_spare,
    pr."SessionId" AS ssid,
    pr."ReportedTimestamp" AS process_time,
    '1'::character(1) AS offline_replace_recovery,
    tr."Self" AS self
   FROM "OfflinePresentations" pr
     JOIN "Trips" tr ON tr."Self" = pr."UpdatedTripHref"
     JOIN ( SELECT tk1."SessionId",
            tk1."Self",
            tk1."MessageId",
            tk1."CardId",
            tk1."CardTypeHref",
            tk1."CardTypeId",
            tk1."LastHistoryAction",
            tk1."LastHistoryActionTimestamp",
            tk1."Patron",
            tk1."ReportedTimestamp",
            tk1."TokenType",
            tk1."State",
            tk1."CardLifecycle",
            tk1."RetailInfo",
            tk1."DeviceArSequenceNumber",
            tk1."DeviceEquipmentName",
            tk1."DeviceEquipmentShortName",
            tk1."DeviceId",
            tk1."DeviceMessageSequenceNumber",
            tk1."DeviceOperatorId",
            tk1."LocationArrayNumber",
            tk1."LocationId",
            tk1."LocationLineId",
            tk1."ServiceOperatingDate",
            tk1."ServiceProviderId",
            tk1."ServiceTimestamp",
            tk1."TokenVector"
           FROM "Tokens" tk1
          WHERE tk1."State" = 'bound'::text AND "substring"(tk1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(tk2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Tokens" tk2
                  WHERE tk1."Self" = tk2."Self" AND tk2."State" = 'bound'::text))) tk ON tr."Token" = tk."Self"
     LEFT JOIN "TripUpgrades" tu ON tr."Self" = tu."TripReportSelf" AND ("left"(tu."TripReportMessageId", 14) = 'Trip Fare Paid'::text OR "left"(tu."TripReportMessageId", 18) = 'Trip Fare Not Paid'::text) AND tu."UpgradeType" = 'exit_mismatch'::text
     LEFT JOIN "Presentations" pr_entry ON tr."EntryHref" = pr_entry."Self"
     LEFT JOIN "OfflinePresentations" ofp ON tr."EntryHref" = ofp."Self"
  WHERE (tr."TripState" = ANY (ARRAY['fare_paid'::text, 'fare_not_paid'::text])) AND pr."ValidatorPassengerDirection" = 'exiting'::text AND pr."UpdatedTripHref" IS NOT NULL AND (COALESCE(tr."FareCharged", 0) > 0 OR COALESCE(tr."FareCharged", 0) = 0 AND (pr."ValidatorFareModeFareFareBypass1" = true AND pr."LocationId" = COALESCE(pr_entry."LocationId", ofp."LocationId") OR pr."ValidatorFareModeFareFareBypass2" = true AND pr."LocationId" <> COALESCE(pr_entry."LocationId", ofp."LocationId")) AND (get_td_generate('SVC EXIT'::text, COALESCE(pr_entry."LocationId", ofp."LocationId"), pr."LocationId") = 0 OR COALESCE(tr."PassAtEntryHref", 'N'::text) = 'N'::text) OR COALESCE(tr."FareCharged", 0) = 0 AND NOT (pr."ValidatorFareModeFareFareBypass1" = true AND pr."LocationId" = COALESCE(pr_entry."LocationId", ofp."LocationId") OR pr."ValidatorFareModeFareFareBypass2" = true AND pr."LocationId" <> COALESCE(pr_entry."LocationId", ofp."LocationId")) AND (get_td_generate('SVC EXIT'::text, COALESCE(pr_entry."LocationId", ofp."LocationId"), pr."LocationId") = 1 OR COALESCE(tr."PassUsedHref", 'N'::text) = 'N'::text)) AND pr."DeviceEquipmentName" IS NOT NULL AND pr."DeviceEquipmentShortName" IS NOT NULL AND pr."DeviceId" IS NOT NULL
UNION ALL
 SELECT 240 AS message_type,
    3 AS payload_version_number,
    0 AS message_sequence_number,
    pr."DeviceId" AS bss_equipment_id,
    100 AS length_of_message,
    153 AS transaction_id,
    tr."ExitTimestamp" AS txn_datetime,
    1 AS service_provider_id,
        CASE
            WHEN NOT pr."ValidatorUnconfirmedCommit" THEN 0
            ELSE 1
        END AS unconfirmed_indicator,
    1 AS project_id,
    "substring"(tk."CardId", 1, 3) AS issuer_id,
        CASE
            WHEN get_svc_exit_entry_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
            CASE
                WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
                WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
                WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
                   FROM "Presentations" p1
                  WHERE p1."Self" = tu."Href")
                ELSE '-1'::integer
            END, pr."LocationId") = 1 AND (get_svc_exit_exit_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
            CASE
                WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
                WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
                WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
                   FROM "Presentations" p1
                  WHERE p1."Self" = tu."Href")
                ELSE '-1'::integer
            END, pr."LocationId") = ANY (ARRAY[31, 32, 33, 34, 35, 53, 54, 55, 56, 57, 58, 59, 60, 61])) THEN tk."CardId"
            WHEN get_svc_exit_entry_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
            CASE
                WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
                WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
                WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
                   FROM "Presentations" p1
                  WHERE p1."Self" = tu."Href")
                ELSE '-1'::integer
            END, pr."LocationId") = 30 AND (get_svc_exit_exit_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
            CASE
                WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
                WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
                WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
                   FROM "Presentations" p1
                  WHERE p1."Self" = tu."Href")
                ELSE '-1'::integer
            END, pr."LocationId") = ANY (ARRAY[36, 37, 38, 39])) THEN tk."CardId"
            ELSE
            CASE
                WHEN COALESCE(tr."PassUsedHref", 'N'::text) = 'N'::text THEN tk."CardId"
                ELSE COALESCE(( SELECT DISTINCT ( SELECT DISTINCT tk_1."CardId"
                           FROM "Tokens" tk_1
                          WHERE tk_1."State" = 'bound'::text AND tk_1."Self" = COALESCE(pa."IssuedAsTokenHref", pa."TransferredTokenHref")) AS "CardId"
                   FROM "Passes" pa
                  WHERE pa."TransferredDirection" = 'from'::text AND pa."Self" = tr."PassUsedHref"), tk."CardId")
            END
        END AS card_id,
    tk."CardTypeId" AS card_type,
    tk."CardLifecycle"::integer AS lifecycle,
    0 AS card_status,
    0 AS block_flag,
    0 AS test_flag,
    0 AS card_sequence_number,
    0 AS card_unblock_sequence_number,
    0 AS personalization_flag,
    0 AS purse_value,
    0 AS lav_service_provider_id,
    0 AS lav_location_code,
    NULL::timestamp without time zone AS lav_txn_datetime,
    0 AS lav_amount,
    0 AS lav_payment,
    0 AS lav_equipmentid,
    0 AS lav_csn,
    COALESCE(tr."FareCharged"::numeric, 0.00) * 100::numeric AS transaction_value,
    pr."ServiceProviderOperatingDate" AS operating_day,
    pr."DeviceEquipmentName" AS equipment_no,
    COALESCE(tr."FareCharged"::numeric, 0.00) * 100::numeric AS fare,
    get_svc_exit_entry_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
        CASE
            WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
            WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
            WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
               FROM "Presentations" p1
              WHERE p1."Self" = tu."Href")
            ELSE '-1'::integer
        END, pr."LocationId") AS entry_location,
        CASE
            WHEN tr."EntryHref" IS NOT NULL AND pr_entry."DeviceEquipmentShortName" IS NOT NULL THEN "right"(pr_entry."DeviceEquipmentShortName", 2)
            WHEN tr."EntryHref" IS NOT NULL AND ofp."DeviceEquipmentShortName" IS NOT NULL THEN "right"(ofp."DeviceEquipmentShortName", 2)
            WHEN tu."Href" IS NOT NULL THEN ( SELECT "right"(p1."DeviceEquipmentShortName", 2) AS "right"
               FROM "Presentations" p1
              WHERE p1."Self" = tu."Href")
            ELSE NULL::text
        END AS entry_equipno,
    get_svc_exit_exit_station(COALESCE(tr."PassUsedHref", 'N'::text), COALESCE(tr."PassAtEntryHref", 'N'::text),
        CASE
            WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
            WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
            WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
               FROM "Presentations" p1
              WHERE p1."Self" = tu."Href")
            ELSE '-1'::integer
        END, pr."LocationId") AS exit_location,
        CASE
            WHEN pr."ValidatorFareModeEntryExitOverride" = true THEN 1
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeTimeOverride" = true THEN 2
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeExcessFareOverride" = true THEN 4
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeFareFareBypass1" = true THEN 8
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeFareFareBypass2" = true THEN 16
            ELSE 0
        END AS fare_mode,
    pr."LocationArrayNumber" AS array_number,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) = 'N'::text AND COALESCE(tr."PassAtEntryHref", 'N'::text) = 'N'::text THEN 0::numeric
            ELSE
            CASE
                WHEN COALESCE(tr."PassUsedHref", 'N'::text) = 'N'::text THEN 0
                ELSE COALESCE(get_fare_collection(COALESCE(pr_entry."LocationId", ofp."LocationId"), pr."LocationId"), 0)
            END::numeric
        END AS fare_collection,
        CASE
            WHEN tr."EntryHref" IS NOT NULL THEN tr."EntryTimestamp"
            WHEN tu."Href" IS NOT NULL THEN tu."Timestamp"
            ELSE NULL::timestamp without time zone
        END AS entry_datetime,
    0 AS trans_mac_version,
    0 AS trans_mac_value,
    1 AS bts_ext_format_version_number,
    0 AS bts_ext_ar_seq_num,
    0 AS bts_ext_device_msg_seq_num,
    pr."LocationId" AS bts_ext_station_id,
    pr."DeviceEquipmentName" AS bts_ext_equipment_id,
    pr."DeviceEquipmentShortName" AS bts_ext_equipment_number,
    pr."LocationArrayNumber" AS bts_ext_array_number,
    0 AS bts_ext_cloud_id_ar_seq,
    0::bigint AS bts_ext_cloud_id_dev_msg_seq,
    0 AS bts_ext_spare,
    pr."SessionId" AS ssid,
    pr."ReportedTimestamp" AS process_time,
    '1'::character(1) AS offline_replace_recovery,
    tr."Self" AS self
   FROM ( SELECT a."ReportedTimestamp",
            a."Self",
            a."MessageId",
            a."CardId",
            a."ValidatorPassengerDirection",
            a."ValidatorFareModeFareFareBypass1",
            a."ValidatorFareModeFareFareBypass2",
            a."LocationId",
            a."UpdatedTripHref",
            a."DeviceEquipmentName",
            a."DeviceEquipmentShortName",
            a."DeviceId",
            a."ValidatorUnconfirmedCommit",
            a."ServiceProviderOperatingDate",
            a."ValidatorFareModeEntryExitOverride",
            a."ValidatorFareModeTimeOverride",
            a."ValidatorFareModeExcessFareOverride",
            a."LocationArrayNumber",
            a."SessionId"
           FROM ( SELECT DISTINCT ON ("OfflinePresentations"."Self") "OfflinePresentations"."ReportedTimestamp",
                    "OfflinePresentations"."Self",
                    "OfflinePresentations"."MessageId",
                    "OfflinePresentations"."CardId",
                    "OfflinePresentations"."ValidatorPassengerDirection",
                    "OfflinePresentations"."ValidatorFareModeFareFareBypass1",
                    "OfflinePresentations"."ValidatorFareModeFareFareBypass2",
                    "OfflinePresentations"."LocationId",
                    "OfflinePresentations"."UpdatedTripHref",
                    "OfflinePresentations"."DeviceEquipmentName",
                    "OfflinePresentations"."DeviceEquipmentShortName",
                    "OfflinePresentations"."DeviceId",
                    "OfflinePresentations"."ValidatorUnconfirmedCommit",
                    "OfflinePresentations"."ServiceProviderOperatingDate",
                    "OfflinePresentations"."ValidatorFareModeEntryExitOverride",
                    "OfflinePresentations"."ValidatorFareModeTimeOverride",
                    "OfflinePresentations"."ValidatorFareModeExcessFareOverride",
                    "OfflinePresentations"."LocationArrayNumber",
                    "OfflinePresentations"."SessionId"
                   FROM "OfflinePresentations"
                  ORDER BY "OfflinePresentations"."Self", "OfflinePresentations"."ReportedTimestamp" DESC) a) pr
     JOIN "Trips" tr ON pr."Self" = tr."ExitHref"
     JOIN ( SELECT tk1."SessionId",
            tk1."Self",
            tk1."MessageId",
            tk1."CardId",
            tk1."CardTypeHref",
            tk1."CardTypeId",
            tk1."LastHistoryAction",
            tk1."LastHistoryActionTimestamp",
            tk1."Patron",
            tk1."ReportedTimestamp",
            tk1."TokenType",
            tk1."State",
            tk1."CardLifecycle",
            tk1."RetailInfo",
            tk1."DeviceArSequenceNumber",
            tk1."DeviceEquipmentName",
            tk1."DeviceEquipmentShortName",
            tk1."DeviceId",
            tk1."DeviceMessageSequenceNumber",
            tk1."DeviceOperatorId",
            tk1."LocationArrayNumber",
            tk1."LocationId",
            tk1."LocationLineId",
            tk1."ServiceOperatingDate",
            tk1."ServiceProviderId",
            tk1."ServiceTimestamp",
            tk1."TokenVector"
           FROM "Tokens" tk1
          WHERE tk1."State" = 'bound'::text AND "substring"(tk1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(tk2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Tokens" tk2
                  WHERE tk1."Self" = tk2."Self" AND tk2."State" = 'bound'::text))) tk ON tr."Token" = tk."Self"
     LEFT JOIN "TripUpgrades" tu ON tr."Self" = tu."TripReportSelf" AND ("left"(tu."TripReportMessageId", 14) = 'Trip Fare Paid'::text OR "left"(tu."TripReportMessageId", 18) = 'Trip Fare Not Paid'::text) AND tu."UpgradeType" = 'exit_mismatch'::text
     LEFT JOIN "Presentations" pr_entry ON tr."EntryHref" = pr_entry."Self"
     LEFT JOIN "OfflinePresentations" ofp ON tr."EntryHref" = ofp."Self"
  WHERE (tr."TripState" = ANY (ARRAY['fare_paid'::text, 'fare_not_paid'::text])) AND pr."ValidatorPassengerDirection" = 'exiting'::text AND (COALESCE(tr."FareCharged", 0) > 0 OR COALESCE(tr."FareCharged", 0) = 0 AND (pr."ValidatorFareModeFareFareBypass1" = true AND pr."LocationId" = COALESCE(pr_entry."LocationId", ofp."LocationId") OR pr."ValidatorFareModeFareFareBypass2" = true AND pr."LocationId" <> COALESCE(pr_entry."LocationId", ofp."LocationId")) AND (get_td_generate('SVC EXIT'::text, COALESCE(pr_entry."LocationId", ofp."LocationId"), pr."LocationId") = 0 OR COALESCE(tr."PassAtEntryHref", 'N'::text) = 'N'::text) OR COALESCE(tr."FareCharged", 0) = 0 AND NOT (pr."ValidatorFareModeFareFareBypass1" = true AND pr."LocationId" = COALESCE(pr_entry."LocationId", ofp."LocationId") OR pr."ValidatorFareModeFareFareBypass2" = true AND pr."LocationId" <> COALESCE(pr_entry."LocationId", ofp."LocationId")) AND (get_td_generate('SVC EXIT'::text, COALESCE(pr_entry."LocationId", ofp."LocationId"), pr."LocationId") = 1 OR COALESCE(tr."PassUsedHref", 'N'::text) = 'N'::text)) AND pr."DeviceEquipmentName" IS NOT NULL AND pr."DeviceEquipmentShortName" IS NOT NULL AND pr."DeviceId" IS NOT NULL;


GRANT SELECT, TRUNCATE, UPDATE, DELETE, REFERENCES, TRIGGER, INSERT ON v_td_svc_exit_offline_replace_recovery TO cloudid;

COMMENT ON VIEW v_td_svc_exit_offline_replace_recovery IS 'Version: 201905211640
Change log
201905211640: CLOUDID-695: change transaction_id 41 to 153 and change offline_replace_recovery Y to 1
201904051604: CLOUDID-685: Add more to check condition >EntryHref< is equal null value to fare_collection is 0
201903061200: CLOUDID-667: Add more to check condition >PassUsedHref< is equal null value to fare_collection is 0
201902221600: CLOUDID-667: Change payload version for SVC Exit
201902061700: Change concept condition of TD generator with Pass, Surcharge is (0, 1) and show actual station
201901291230: Convert MessageId to bigint type
201901251000: Add >SELF< column by request
201901161000 : Performance tuning query: Replace >SELECT DISTINCT ON< to >MessageId<
201811121800 : Support new station for BMA4 (54-63) and Zero fare
201811121500 : Change query to get data for entry/exit to calculate with fare_collection
201811091500 : Fixed replacement card when IssueAtPass is null
201811091000 : Fixed fare collection to use get_fare_collection function
201811051000 : Fixed for check special case to use new card id
201811021400 : Fixed for check special case to use old card id
201811011200 : CLOUDIDTST-587 Remove exit offline had recovery by offline log and not had next entry online -- Cancel
201810311700 : CLOUDID-618  Check original card id for support replacement
201810081633 : CLOUDID-580  Add case entry online --> exit offline not recovery
201809031600 : CLOUDID-568  initial version
';
COMMIT;
