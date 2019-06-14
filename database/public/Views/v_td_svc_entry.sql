DROP VIEW IF EXISTS v_td_svc_entry CASCADE;

CREATE OR REPLACE VIEW v_td_svc_entry
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
        CASE
            WHEN pr_rcv."DeviceId" IS NOT NULL THEN pr_rcv."DeviceId"
            WHEN pr."DeviceId" IS NOT NULL THEN pr."DeviceId"
            ELSE ofp."DeviceId"
        END AS bss_equipment_id,
    64 AS length_of_message,
    151 AS transaction_id,
    tr."EntryTimestamp" AS txn_datetime,
    1 AS service_provider_id,
        CASE
            WHEN pr."ValidatorUnconfirmedCommit" THEN 1
            WHEN pr_exit."ValidatorUnconfirmedCommit" THEN 1
            WHEN ofp."ValidatorUnconfirmedCommit" THEN 1
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
        CASE
            WHEN pr."ServiceProviderOperatingDate" IS NOT NULL THEN pr."ServiceProviderOperatingDate"
            ELSE ofp."ServiceProviderOperatingDate"
        END AS operating_day,
        CASE
            WHEN pr_rcv."DeviceEquipmentName" IS NOT NULL THEN pr_rcv."DeviceEquipmentName"
            WHEN pr."DeviceEquipmentName" IS NOT NULL THEN pr."DeviceEquipmentName"
            ELSE ofp."DeviceEquipmentName"
        END AS equipment_no,
        CASE
            WHEN pr."LocationId" IS NOT NULL THEN pr."LocationId"
            ELSE ofp."LocationId"
        END AS entry_location,
        CASE
            WHEN pr."ValidatorFareModeEntryExitOverride" = true THEN 1
            WHEN ofp."ValidatorFareModeEntryExitOverride" = true THEN 1
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeTimeOverride" = true THEN 2
            WHEN ofp."ValidatorFareModeTimeOverride" = true THEN 2
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeExcessFareOverride" = true THEN 4
            WHEN ofp."ValidatorFareModeExcessFareOverride" = true THEN 4
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeFareFareBypass1" = true THEN 8
            WHEN ofp."ValidatorFareModeFareFareBypass1" = true THEN 8
            ELSE 0
        END +
        CASE
            WHEN pr."ValidatorFareModeFareFareBypass2" = true THEN 16
            WHEN ofp."ValidatorFareModeFareFareBypass2" = true THEN 16
            ELSE 0
        END AS fare_mode,
        CASE
            WHEN pr_rcv."LocationArrayNumber" IS NOT NULL THEN pr_rcv."LocationArrayNumber"
            WHEN pr."LocationArrayNumber" IS NOT NULL THEN pr."LocationArrayNumber"
            ELSE ofp."LocationArrayNumber"
        END AS array_number,
    0 AS trans_mac_version,
    0 AS trans_mac_value,
    1 AS bts_ext_format_version_number,
    0 AS bts_ext_ar_seq_num,
    0 AS bts_ext_device_msg_seq_num,
        CASE
            WHEN pr."LocationId" IS NOT NULL THEN pr."LocationId"
            ELSE ofp."LocationId"
        END AS bts_ext_station_id,
        CASE
            WHEN pr_rcv."DeviceEquipmentName" IS NOT NULL THEN pr_rcv."DeviceEquipmentName"
            WHEN pr."DeviceEquipmentName" IS NOT NULL THEN pr."DeviceEquipmentName"
            ELSE ofp."DeviceEquipmentName"
        END AS bts_ext_equipment_id,
        CASE
            WHEN pr_rcv."DeviceEquipmentShortName" IS NOT NULL THEN pr_rcv."DeviceEquipmentShortName"
            WHEN pr."DeviceEquipmentShortName" IS NOT NULL THEN pr."DeviceEquipmentShortName"
            ELSE ofp."DeviceEquipmentShortName"
        END AS bts_ext_equipment_number,
        CASE
            WHEN pr_rcv."LocationArrayNumber" IS NOT NULL THEN pr_rcv."LocationArrayNumber"
            WHEN pr."LocationArrayNumber" IS NOT NULL THEN pr."LocationArrayNumber"
            ELSE ofp."LocationArrayNumber"
        END AS bts_ext_array_number,
    0 AS bts_ext_cloud_id_ar_seq,
    0::bigint AS bts_ext_cloud_id_dev_msg_seq,
    0 AS bts_ext_spare,
    tr."SessionId" AS ssid,
        CASE
            WHEN pr."ReportedTimestamp" IS NOT NULL THEN pr."ReportedTimestamp"
            ELSE ofp."ReportedTimestamp"
        END AS process_time,
    '0'::character(1) AS offline_replace_recovery,
    tr."Self" AS self
   FROM "Trips" tr
     LEFT JOIN "Presentations" pr ON tr."EntryHref" = pr."Self"
     LEFT JOIN ( SELECT pr1."SessionId",
            pr1."Self",
            pr1."MessageId",
            pr1."CardId",
            pr1."DeviceId",
            pr1."LinkedPresentation",
            pr1."LocationArrayNumber",
            pr1."LocationId",
            pr1."LocationLineId",
            pr1."ReportedTimestamp",
            pr1."ServiceProviderId",
            pr1."ServiceProviderOperatingDate",
            pr1."ServiceProviderTimestamp",
            pr1."TokenType",
            pr1."UpgradeReason",
            pr1."ValidatorFareModeEntryExitOverride",
            pr1."DeviceEquipmentName",
            pr1."DeviceEquipmentShortName",
            pr1."UpgradePresentedOnline",
            pr1."UpgradeUnconfirmedCommit",
            pr1."ValidatorFareModeExcessFareOverride",
            pr1."ValidatorFareModeFareChargedOverride",
            pr1."ValidatorFareModeFareFareBypass1",
            pr1."ValidatorFareModeFareFareBypass2",
            pr1."ValidatorFareModeTimeOverride",
            pr1."ValidatorPassengerDirection",
            pr1."ValidatorPresentedOnline",
            pr1."ValidatorUnconfirmedCommit",
            pr1."CardLifecycle",
            pr1."DeviceMessageSequenceNumber",
            pr1."DeviceArSequenceNumber",
            pr1."OperatorId"
           FROM "Presentations" pr1
          WHERE "substring"(pr1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT min("substring"(pr2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Presentations" pr2
                  WHERE pr1."Self" = pr2."Self"))) pr_exit ON tr."ExitHref" = pr_exit."Self"
     LEFT JOIN "OfflinePresentations" ofp ON tr."EntryHref" = ofp."Self"
     LEFT JOIN "OfflinePresentations" ofp_exit ON tr."ExitHref" = ofp_exit."Self"
     LEFT JOIN ( SELECT DISTINCT "Trips"."Self",
            "Trips"."OfflineTripHref",
            "Trips"."EntryHref"
           FROM "Trips"
          WHERE "Trips"."OfflineTripHref" IS NOT NULL
          ORDER BY "Trips"."Self") tr_rcv ON tr."OfflineTripHref" = tr_rcv."OfflineTripHref" AND tr_rcv."Self" <> tr."Self"
     LEFT JOIN "Presentations" pr_rcv ON tr_rcv."EntryHref" = pr_rcv."Self"
     JOIN ( SELECT tk1."Self",
            tk1."CardId",
            tk1."CardTypeId",
            tk1."CardLifecycle"
           FROM "Tokens" tk1
          WHERE tk1."State" = 'bound'::text AND "substring"(tk1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(tk2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Tokens" tk2
                  WHERE tk1."Self" = tk2."Self" AND tk2."State" = 'bound'::text))) tk ON tr."Token" = tk."Self"
  WHERE (tr."TripState" = ANY (ARRAY['fare_paid'::text, 'fare_not_paid'::text])) AND (get_td_generate('SVC ENTRY'::text, COALESCE(pr."LocationId", ofp."LocationId"), 0) = 1 OR get_td_generate('SVC ENTRY'::text, COALESCE(pr."LocationId", ofp."LocationId"), 0) = 0 AND COALESCE(tr."PassAtEntryHref", 'N'::text) = 'N'::text) AND (pr."DeviceEquipmentName" IS NOT NULL AND pr."DeviceEquipmentShortName" IS NOT NULL AND pr."DeviceId" IS NOT NULL OR pr_rcv."DeviceEquipmentName" IS NOT NULL AND pr_rcv."DeviceEquipmentShortName" IS NOT NULL AND pr_rcv."DeviceId" IS NOT NULL OR ofp."DeviceEquipmentName" IS NOT NULL AND ofp."DeviceEquipmentShortName" IS NOT NULL AND ofp."DeviceId" IS NOT NULL);


GRANT SELECT, TRUNCATE, UPDATE, DELETE, REFERENCES, TRIGGER, INSERT ON v_td_svc_entry TO cloudid;

COMMENT ON VIEW v_td_svc_entry IS 'Version: Version: 201905211640
Change log
201905211640: CLOUDID-695: change transaction_id 21 to 151 and change offline_replace_recovery N to 0
201902041800: Change concept condition of TD generator with Pass and Surcharge is (0, 1)
201901291230: Convert MessageId to bigint type
201901251000: Add >SELF< column by request
201901161000: Performance tuning query: Replace >SELECT DISTINCT ON< to >MessageId<
201811211230: Fixed STP Entry TD lost when Transaction Recovery by Exit Offline
201811091500: Fixed replacement card when IssueAtPass is null
201810311700: Check original card id for support replacement
201808231600: CLOUDID-568
201808311900: CLOUDID-552
201808231600: CLOUDID-552
201808141900: CLOUDID-552
201806181800: CLOUDIDTST-437
201803051200: CLOUDIDTST-153
201802091200:  CLOUDIDTST-79
2018-02-07: Fixed CLOUDIDTST-66
';
COMMIT;
