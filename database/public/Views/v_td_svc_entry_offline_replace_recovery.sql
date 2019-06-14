DROP VIEW IF EXISTS v_td_svc_entry_offline_replace_recovery CASCADE;

CREATE OR REPLACE VIEW v_td_svc_entry_offline_replace_recovery
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
  operating_day,
  equipment_no,
  entry_location,
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
  process_time,
  offline_replace_recovery,
  self
)
AS 
 SELECT 240 AS message_type,
    1 AS payload_version_number,
    0 AS message_sequence_number,
    pr."DeviceId" AS bss_equipment_id,
    64 AS length_of_message,
    151 AS transaction_id,
    tr."EntryTimestamp" AS txn_datetime,
    1 AS service_provider_id,
        CASE
            WHEN pr."ValidatorUnconfirmedCommit" THEN 1
            WHEN pr_exit."ValidatorUnconfirmedCommit" THEN 1
            WHEN ofp_exit."ValidatorUnconfirmedCommit" THEN 1
            ELSE 0
        END AS unconfirmed_indicator,
    1 AS project_id,
    "substring"(tk."CardId", 1, 3) AS issuer_id,
    COALESCE(
        CASE
            WHEN COALESCE(tr."PassUsedHref", 'N'::text) = 'N'::text THEN tk."CardId"
            ELSE COALESCE(( SELECT DISTINCT ( SELECT DISTINCT tk_1."CardId"
                       FROM "Tokens" tk_1
                      WHERE tk_1."State" = 'bound'::text AND tk_1."Self" = COALESCE(pa."IssuedAsTokenHref", pa."TransferredTokenHref")) AS "CardId"
               FROM "Passes" pa
              WHERE pa."TransferredDirection" = 'from'::text AND pa."Self" = tr."PassUsedHref"), tk."CardId")
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
    pr."ServiceProviderOperatingDate" AS operating_day,
    pr."DeviceEquipmentName" AS equipment_no,
    pr."LocationId" AS entry_location,
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
    pr."ReportedTimestamp" AS process_time,
    '1'::character(1) AS offline_replace_recovery,
    tr."Self" AS self
   FROM "OfflinePresentations" pr
     JOIN "Trips" tr ON tr."Self" = pr."UpdatedTripHref"
     LEFT JOIN "Presentations" pr_exit ON tr."ExitHref" = pr_exit."Self"
     LEFT JOIN "OfflinePresentations" ofp_exit ON tr."ExitHref" = ofp_exit."Self"
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
  WHERE (tr."TripState" = ANY (ARRAY['fare_paid'::text, 'fare_not_paid'::text])) AND pr."ValidatorPassengerDirection" = 'entering'::text AND pr."UpdatedTripHref" IS NOT NULL AND (get_td_generate('SVC ENTRY'::text, pr."LocationId", 0) = 1 OR get_td_generate('SVC ENTRY'::text, pr."LocationId", 0) = 0 AND COALESCE(tr."PassAtEntryHref", 'N'::text) = 'N'::text) AND pr."DeviceEquipmentName" IS NOT NULL AND pr."DeviceEquipmentShortName" IS NOT NULL AND pr."DeviceId" IS NOT NULL;


GRANT SELECT, TRUNCATE, UPDATE, DELETE, REFERENCES, TRIGGER, INSERT ON v_td_svc_entry_offline_replace_recovery TO cloudid;

COMMENT ON VIEW v_td_svc_entry_offline_replace_recovery IS 'Version: Version: 201905211640
Change log
201905211640: CLOUDID-695: change transaction_id 21 to 151 and change offline_replace_recovery Y to 1
201902041800: Change concept condition of TD generator with Pass and Surcharge is (0, 1)
201901291230: Convert MessageId to bigint type
201901251000: Add >SELF< column by request
201901161000: Performance tuning query: Replace >SELECT DISTINCT ON< to >MessageId<
201809031600: CLOUDID-568 initial version
201810311700: Check original card id for support replacement
201811091500: Fixed replacement card when IssueAtPass is null
';
COMMIT;
