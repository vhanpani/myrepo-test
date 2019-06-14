DROP VIEW IF EXISTS v_td_stp_exit_offline_replace_recovery CASCADE;

CREATE OR REPLACE VIEW v_td_stp_exit_offline_replace_recovery
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
  pin,
  sp_id,
  pass_status,
  passtype,
  pass_valid_startdate,
  pass_valid_enddate,
  pass_valid_trip_counter,
  pass_remain_trip,
  operating_day,
  equipment_no,
  fare,
  entry_location,
  entry_equipno,
  exit_location,
  fare_mode,
  array_number,
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
  entry_datetime,
  process_time,
  offline_replace_recovery,
  self
)
AS 
 SELECT 240 AS message_type,
    1 AS payload_version_number,
    0 AS message_sequence_number,
    pr."DeviceId" AS bss_equipment_id,
    110 AS length_of_message,
    154 AS transaction_id,
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
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN
            CASE
                WHEN COALESCE(pa."TransferredDirection", 'N'::text) = 'from'::text THEN ( SELECT tk3."CardId"
                   FROM "Tokens" tk3
                  WHERE tk3."Self" = COALESCE(pa."IssuedAsTokenHref", pa."TransferredTokenHref")
                  GROUP BY tk3."CardId"
                 LIMIT 1)
                ELSE tk."CardId"
            END
            ELSE
            CASE
                WHEN COALESCE(pa_entry."TransferredDirection", 'N'::text) = 'from'::text THEN ( SELECT tk3."CardId"
                   FROM "Tokens" tk3
                  WHERE tk3."Self" = COALESCE(pa_entry."IssuedAsTokenHref", pa."TransferredTokenHref")
                  GROUP BY tk3."CardId"
                 LIMIT 1)
                ELSE tk."CardId"
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
    COALESCE(
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN
            CASE
                WHEN COALESCE(pa."IssuedAsPassIdentificationNumber", 0::oid)::text <> 0::text THEN pa."IssuedAsPassIdentificationNumber"::text
                ELSE (( SELECT DISTINCT p3."PassIdentificationNumber"
                   FROM "Passes" p3
                  WHERE p3."Self" = pa."TransferredPassHref"))::text
            END
            ELSE pa_entry."IssuedAsPassIdentificationNumber"::text
        END,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN
            CASE
                WHEN COALESCE(pa."PassIdentificationNumber", 0::oid)::text <> 0::text THEN pa."PassIdentificationNumber"::text
                ELSE (( SELECT DISTINCT p3."PassIdentificationNumber"
                   FROM "Passes" p3
                  WHERE p3."Self" = pa."TransferredPassHref"))::text
            END
            ELSE pa_entry."PassIdentificationNumber"::text
        END) AS pin,
    1 AS sp_id,
        CASE
            WHEN pa."State" = 'issued'::text OR pa_entry."State" = 'issued'::text THEN 1
            WHEN pa."State" = 'activated'::text OR pa_entry."State" = 'activated'::text THEN 2
            WHEN pa."State" = 'expired'::text OR pa_entry."State" = 'expired'::text THEN 3
            WHEN pa."State" = 'reversed'::text OR pa_entry."State" = 'reversed'::text THEN 4
            WHEN pa."State" = 'exhausted'::text OR pa_entry."State" = 'exhausted'::text THEN 5
            WHEN pa."State" = 'transferred'::text OR pa_entry."State" = 'transferred'::text THEN 6
            WHEN pa."State" = 'canceled'::text OR pa_entry."State" = 'canceled'::text THEN 7
            ELSE NULL::integer
        END AS pass_status,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN pa."PassType"
            ELSE pa_entry."PassType"
        END AS passtype,
        CASE
            WHEN pa."ValidityDurationMode" = 'from_issue'::text THEN pa."OperatingDay"::timestamp without time zone
            WHEN pa."ValidityDurationMode" = 'from_first_use'::text THEN pa."FixedStartDate"
            WHEN pa."ValidityDurationMode" = 'fixed_date'::text THEN pa."FixedStartDate"
            WHEN pa_entry."ValidityDurationMode" = 'from_issue'::text THEN pa_entry."OperatingDay"::timestamp without time zone
            WHEN pa_entry."ValidityDurationMode" = 'from_first_use'::text THEN pa_entry."FixedStartDate"
            WHEN pa_entry."ValidityDurationMode" = 'fixed_date'::text THEN pa_entry."FixedStartDate"
            ELSE NULL::timestamp without time zone
        END AS pass_valid_startdate,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN pa."FixedEndDate"
            ELSE pa_entry."FixedEndDate"
        END AS pass_valid_enddate,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN tr."PassUsedDailyTripNumber"
            ELSE tr."PassAtEntryDailyTripNum"
        END AS pass_valid_trip_counter,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN tr."PassUsedTripsRemaining"
            ELSE tr."PassAtEntryTripsRemaining"
        END AS pass_remain_trip,
    pr."ServiceProviderOperatingDate" AS operating_day,
    pr."DeviceEquipmentName" AS equipment_no,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN COALESCE(pa."AverageTripFare"::numeric, 0.00)
            ELSE 0::numeric
        END AS fare,
        CASE
            WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
            WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
            WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
               FROM "Presentations" p1
              WHERE p1."Self" = tu."Href")
            ELSE 0
        END AS entry_location,
        CASE
            WHEN tr."EntryHref" IS NOT NULL AND pr_entry."DeviceEquipmentShortName" IS NOT NULL THEN "right"(pr_entry."DeviceEquipmentShortName", 2)
            WHEN tr."EntryHref" IS NOT NULL AND ofp."DeviceEquipmentShortName" IS NOT NULL THEN "right"(ofp."DeviceEquipmentShortName", 2)
            WHEN tu."Href" IS NOT NULL THEN ( SELECT "right"(p1."DeviceEquipmentShortName", 2) AS "right"
               FROM "Presentations" p1
              WHERE p1."Self" = tu."Href")
            ELSE NULL::text
        END AS entry_equipno,
    pr."LocationId" AS exit_location,
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
        CASE
            WHEN tr."EntryHref" IS NOT NULL THEN tr."EntryTimestamp"
            WHEN tu."Href" IS NOT NULL THEN tu."Timestamp"
            ELSE NULL::timestamp without time zone
        END AS entry_datetime,
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
     LEFT JOIN ( SELECT pa1."TransferredDirection",
            pa1."IssuedAsTokenHref",
            pa1."TransferredTokenHref",
            pa1."IssuedAsPassIdentificationNumber",
            pa1."TransferredPassHref",
            pa1."PassIdentificationNumber",
            pa1."State",
            pa1."PassType",
            pa1."ValidityDurationMode",
            pa1."OperatingDay",
            pa1."FixedStartDate",
            pa1."FixedEndDate",
            pa1."AverageTripFare",
            pa1."Self"
           FROM "Passes" pa1
          WHERE pa1."State" = 'activated'::text AND "substring"(pa1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(pa2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Passes" pa2
                  WHERE pa1."Self" = pa2."Self" AND pa2."State" = 'activated'::text))) pa ON tr."PassUsedHref" = pa."Self"
     LEFT JOIN "Presentations" pr_entry ON tr."EntryHref" = pr_entry."Self"
     LEFT JOIN "OfflinePresentations" ofp ON tr."EntryHref" = ofp."Self"
     LEFT JOIN ( SELECT pa1."TransferredDirection",
            pa1."IssuedAsTokenHref",
            pa1."TransferredTokenHref",
            pa1."IssuedAsPassIdentificationNumber",
            pa1."TransferredPassHref",
            pa1."PassIdentificationNumber",
            pa1."State",
            pa1."PassType",
            pa1."ValidityDurationMode",
            pa1."OperatingDay",
            pa1."FixedStartDate",
            pa1."FixedEndDate",
            pa1."AverageTripFare",
            pa1."Self"
           FROM "Passes" pa1
          WHERE pa1."State" = 'activated'::text AND "substring"(pa1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(pa2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Passes" pa2
                  WHERE pa1."Self" = pa2."Self" AND pa2."State" = 'activated'::text))) pa_entry ON tr."PassAtEntryHref" = pa_entry."Self"
     LEFT JOIN "TripUpgrades" tu ON tr."Self" = tu."TripReportSelf" AND ("left"(tu."TripReportMessageId", 14) = 'Trip Fare Paid'::text OR "left"(tu."TripReportMessageId", 18) = 'Trip Fare Not Paid'::text) AND tu."UpgradeType" = 'exit_mismatch'::text
  WHERE (tr."TripState" = ANY (ARRAY['fare_paid'::text, 'fare_not_paid'::text])) AND pr."ValidatorPassengerDirection" = 'exiting'::text AND COALESCE(pr."UpdatedTripHref", 'N'::text) <> 'N'::text AND (COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text OR COALESCE(tr."PassAtEntryHref", 'N'::text) <> 'N'::text AND (pr."ValidatorFareModeFareFareBypass1" = true AND pr."LocationId" = COALESCE(pr_entry."LocationId", ofp."LocationId") OR pr."ValidatorFareModeFareFareBypass2" = true AND pr."LocationId" <> COALESCE(pr_entry."LocationId", ofp."LocationId")) AND get_td_generate('STP EXIT'::text, COALESCE(pr_entry."LocationId", ofp."LocationId"), pr."LocationId") = 1) AND pr."DeviceEquipmentName" IS NOT NULL AND pr."DeviceEquipmentShortName" IS NOT NULL AND pr."DeviceId" IS NOT NULL
UNION ALL
 SELECT 240 AS message_type,
    1 AS payload_version_number,
    0 AS message_sequence_number,
    pr."DeviceId" AS bss_equipment_id,
    110 AS length_of_message,
    154 AS transaction_id,
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
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN
            CASE
                WHEN COALESCE(pa."TransferredDirection", 'N'::text) = 'from'::text THEN ( SELECT tk3."CardId"
                   FROM "Tokens" tk3
                  WHERE tk3."Self" = COALESCE(pa."IssuedAsTokenHref", pa."TransferredTokenHref")
                  GROUP BY tk3."CardId"
                 LIMIT 1)
                ELSE tk."CardId"
            END
            ELSE
            CASE
                WHEN COALESCE(pa_entry."TransferredDirection", 'N'::text) = 'from'::text THEN ( SELECT tk3."CardId"
                   FROM "Tokens" tk3
                  WHERE tk3."Self" = COALESCE(pa_entry."IssuedAsTokenHref", pa."TransferredTokenHref")
                  GROUP BY tk3."CardId"
                 LIMIT 1)
                ELSE tk."CardId"
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
    COALESCE(
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN
            CASE
                WHEN COALESCE(pa."IssuedAsPassIdentificationNumber", 0::oid)::text <> 0::text THEN pa."IssuedAsPassIdentificationNumber"::text
                ELSE (( SELECT DISTINCT p3."PassIdentificationNumber"
                   FROM "Passes" p3
                  WHERE p3."Self" = pa."TransferredPassHref"))::text
            END
            ELSE pa_entry."IssuedAsPassIdentificationNumber"::text
        END,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN
            CASE
                WHEN COALESCE(pa."PassIdentificationNumber", 0::oid)::text <> 0::text THEN pa."PassIdentificationNumber"::text
                ELSE (( SELECT DISTINCT p3."PassIdentificationNumber"
                   FROM "Passes" p3
                  WHERE p3."Self" = pa."TransferredPassHref"))::text
            END
            ELSE pa_entry."PassIdentificationNumber"::text
        END) AS pin,
    1 AS sp_id,
        CASE
            WHEN pa."State" = 'issued'::text OR pa_entry."State" = 'issued'::text THEN 1
            WHEN pa."State" = 'activated'::text OR pa_entry."State" = 'activated'::text THEN 2
            WHEN pa."State" = 'expired'::text OR pa_entry."State" = 'expired'::text THEN 3
            WHEN pa."State" = 'reversed'::text OR pa_entry."State" = 'reversed'::text THEN 4
            WHEN pa."State" = 'exhausted'::text OR pa_entry."State" = 'exhausted'::text THEN 5
            WHEN pa."State" = 'transferred'::text OR pa_entry."State" = 'transferred'::text THEN 6
            WHEN pa."State" = 'canceled'::text OR pa_entry."State" = 'canceled'::text THEN 7
            ELSE NULL::integer
        END AS pass_status,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN pa."PassType"
            ELSE pa_entry."PassType"
        END AS passtype,
        CASE
            WHEN pa."ValidityDurationMode" = 'from_issue'::text THEN pa."OperatingDay"::timestamp without time zone
            WHEN pa."ValidityDurationMode" = 'from_first_use'::text THEN pa."FixedStartDate"
            WHEN pa."ValidityDurationMode" = 'fixed_date'::text THEN pa."FixedStartDate"
            WHEN pa_entry."ValidityDurationMode" = 'from_issue'::text THEN pa_entry."OperatingDay"::timestamp without time zone
            WHEN pa_entry."ValidityDurationMode" = 'from_first_use'::text THEN pa_entry."FixedStartDate"
            WHEN pa_entry."ValidityDurationMode" = 'fixed_date'::text THEN pa_entry."FixedStartDate"
            ELSE NULL::timestamp without time zone
        END AS pass_valid_startdate,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN pa."FixedEndDate"
            ELSE pa_entry."FixedEndDate"
        END AS pass_valid_enddate,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN tr."PassUsedDailyTripNumber"
            ELSE tr."PassAtEntryDailyTripNum"
        END AS pass_valid_trip_counter,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN tr."PassUsedTripsRemaining"
            ELSE tr."PassAtEntryTripsRemaining"
        END AS pass_remain_trip,
    pr."ServiceProviderOperatingDate" AS operating_day,
    pr."DeviceEquipmentName" AS equipment_no,
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text THEN COALESCE(pa."AverageTripFare"::numeric, 0.00)
            ELSE 0::numeric
        END AS fare,
        CASE
            WHEN tr."EntryHref" IS NOT NULL AND pr_entry."LocationId" IS NOT NULL THEN pr_entry."LocationId"
            WHEN tr."EntryHref" IS NOT NULL THEN ofp."LocationId"
            WHEN tu."Href" IS NOT NULL THEN ( SELECT p1."LocationId"
               FROM "Presentations" p1
              WHERE p1."Self" = tu."Href")
            ELSE 0
        END AS entry_location,
        CASE
            WHEN tr."EntryHref" IS NOT NULL AND pr_entry."DeviceEquipmentShortName" IS NOT NULL THEN "right"(pr_entry."DeviceEquipmentShortName", 2)
            WHEN tr."EntryHref" IS NOT NULL AND ofp."DeviceEquipmentShortName" IS NOT NULL THEN "right"(ofp."DeviceEquipmentShortName", 2)
            WHEN tu."Href" IS NOT NULL THEN ( SELECT "right"(p1."DeviceEquipmentShortName", 2) AS "right"
               FROM "Presentations" p1
              WHERE p1."Self" = tu."Href")
            ELSE NULL::text
        END AS entry_equipno,
    pr."LocationId" AS exit_location,
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
        CASE
            WHEN tr."EntryHref" IS NOT NULL THEN tr."EntryTimestamp"
            WHEN tu."Href" IS NOT NULL THEN tu."Timestamp"
            ELSE NULL::timestamp without time zone
        END AS entry_datetime,
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
     JOIN "Trips" tr ON tr."ExitHref" = pr."Self"
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
     LEFT JOIN ( SELECT pa1."SessionId",
            pa1."Self",
            pa1."MessageId",
            pa1."AverageTripFare",
            pa1."FirstUseExpiryDate",
            pa1."FixedEndDate",
            pa1."FixedStartDate",
            pa1."IsRefundable",
            pa1."LastTripDate",
            pa1."MaxDailyTrips",
            pa1."MaxTrips",
            pa1."NumDailyTrips",
            pa1."NumTrips",
            pa1."PassType",
            pa1."ReportedTimestamp",
            pa1."State",
            pa1."TimeOfIssue",
            pa1."Token",
            pa1."ValidityDuration",
            pa1."DateOfFirstUse",
            pa1."DisruptionDays",
            pa1."ValidityDurationMode",
            pa1."OperatingDay",
            pa1."Price",
            pa1."PassSlotNum",
            pa1."PassIdentificationNumber",
            pa1."TransactionId",
            pa1."IssuedAsPassHref",
            pa1."IssuedAsPassIdentificationNumber",
            pa1."IssuedAsTokenHref",
            pa1."TransferredDirection",
            pa1."TransferredPassHref",
            pa1."TransferredTokenHref"
           FROM "Passes" pa1
          WHERE pa1."State" = 'activated'::text AND "substring"(pa1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(pa2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Passes" pa2
                  WHERE pa1."Self" = pa2."Self" AND pa2."State" = 'activated'::text))) pa ON tr."PassUsedHref" = pa."Self"
     LEFT JOIN "Presentations" pr_entry ON tr."EntryHref" = pr_entry."Self"
     LEFT JOIN "OfflinePresentations" ofp ON tr."EntryHref" = ofp."Self"
     LEFT JOIN ( SELECT pa1."SessionId",
            pa1."Self",
            pa1."MessageId",
            pa1."AverageTripFare",
            pa1."FirstUseExpiryDate",
            pa1."FixedEndDate",
            pa1."FixedStartDate",
            pa1."IsRefundable",
            pa1."LastTripDate",
            pa1."MaxDailyTrips",
            pa1."MaxTrips",
            pa1."NumDailyTrips",
            pa1."NumTrips",
            pa1."PassType",
            pa1."ReportedTimestamp",
            pa1."State",
            pa1."TimeOfIssue",
            pa1."Token",
            pa1."ValidityDuration",
            pa1."DateOfFirstUse",
            pa1."DisruptionDays",
            pa1."ValidityDurationMode",
            pa1."OperatingDay",
            pa1."Price",
            pa1."PassSlotNum",
            pa1."PassIdentificationNumber",
            pa1."TransactionId",
            pa1."IssuedAsPassHref",
            pa1."IssuedAsPassIdentificationNumber",
            pa1."IssuedAsTokenHref",
            pa1."TransferredDirection",
            pa1."TransferredPassHref",
            pa1."TransferredTokenHref"
           FROM "Passes" pa1
          WHERE pa1."State" = 'activated'::text AND "substring"(pa1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(pa2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Passes" pa2
                  WHERE pa1."Self" = pa2."Self" AND pa2."State" = 'activated'::text))) pa_entry ON tr."PassAtEntryHref" = pa_entry."Self"
     LEFT JOIN "TripUpgrades" tu ON tr."Self" = tu."TripReportSelf" AND ("left"(tu."TripReportMessageId", 14) = 'Trip Fare Paid'::text OR "left"(tu."TripReportMessageId", 18) = 'Trip Fare Not Paid'::text) AND tu."UpgradeType" = 'exit_mismatch'::text
  WHERE (tr."TripState" = ANY (ARRAY['fare_paid'::text, 'fare_not_paid'::text])) AND pr."ValidatorPassengerDirection" = 'exiting'::text AND (COALESCE(tr."PassUsedHref", 'N'::text) <> 'N'::text OR COALESCE(tr."PassAtEntryHref", 'N'::text) <> 'N'::text AND (pr."ValidatorFareModeFareFareBypass1" = true AND pr."LocationId" = COALESCE(pr_entry."LocationId", ofp."LocationId") OR pr."ValidatorFareModeFareFareBypass2" = true AND pr."LocationId" <> COALESCE(pr_entry."LocationId", ofp."LocationId")) AND get_td_generate('STP EXIT'::text, COALESCE(pr_entry."LocationId", ofp."LocationId"), pr."LocationId") = 1) AND pr."DeviceEquipmentName" IS NOT NULL AND pr."DeviceEquipmentShortName" IS NOT NULL AND pr."DeviceId" IS NOT NULL;


GRANT SELECT, TRUNCATE, UPDATE, DELETE, REFERENCES, TRIGGER, INSERT ON v_td_stp_exit_offline_replace_recovery TO cloudid;

COMMENT ON VIEW v_td_stp_exit_offline_replace_recovery IS 'Version: Version: 201905211640
Change log
201905211640: CLOUDID-695: change transaction_id 43 to 154 and change pass_status and change offline_replace_recovery Y to 1
201902081200: Change concept condition of TD generator with Pass
201901291230: Convert MessageId to bigint type
201901251000: Add >SELF< column by request
201901161000: Performance tuning query: Replace >SELECT DISTINCT ON< to >MessageId<
201811121500: Support new station for BMA4 (54-63) and Zero fare
201811091500: Fixed replacement card when IssueAtPass is null
201811011500: Remove UpdatedTripHref condition
201810301700: Change PassIdentificationNumber to IssuedAsPassIdentificationNumber support replacement
201810081633 : CLOUDID-580  Add case entry online --> exit offline not recovery
201809031600 :  CLOUDID-568 initial version 
';
COMMIT;
