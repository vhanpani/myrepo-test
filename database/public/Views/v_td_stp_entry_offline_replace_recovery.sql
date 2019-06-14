DROP VIEW IF EXISTS v_td_stp_entry_offline_replace_recovery CASCADE;

CREATE OR REPLACE VIEW v_td_stp_entry_offline_replace_recovery
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
    80 AS length_of_message,
    152 AS transaction_id,
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
        CASE
            WHEN COALESCE(pa."TransferredDirection", 'N'::text) = 'from'::text THEN ( SELECT tk3."CardId"
               FROM "Tokens" tk3
              WHERE tk3."Self" = COALESCE(pa."IssuedAsTokenHref", pa."TransferredTokenHref")
              GROUP BY tk3."CardId"
             LIMIT 1)
            ELSE tk."CardId"
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
    COALESCE(
        CASE
            WHEN COALESCE(pa."IssuedAsPassIdentificationNumber", 0::oid)::text <> 0::text THEN pa."IssuedAsPassIdentificationNumber"::text
            ELSE (( SELECT DISTINCT p3."PassIdentificationNumber"
               FROM "Passes" p3
              WHERE p3."Self" = pa."TransferredPassHref"))::text
        END, pa."PassIdentificationNumber"::text) AS pin,
    1 AS sp_id,
        CASE pa."State"
            WHEN 'issued'::text THEN 1
            WHEN 'activated'::text THEN 2
            WHEN 'expired'::text THEN 3
            WHEN 'reversed'::text THEN 4
            WHEN 'exhausted'::text THEN 5
            WHEN 'transferred'::text THEN 6
            WHEN 'canceled'::text THEN 7
            ELSE NULL::integer
        END AS pass_status,
    pa."PassType" AS passtype,
        CASE
            WHEN pa."ValidityDurationMode" = 'from_issue'::text THEN pa."OperatingDay"::timestamp without time zone
            WHEN pa."ValidityDurationMode" = 'from_first_use'::text THEN pa."FixedStartDate"
            WHEN pa."ValidityDurationMode" = 'fixed_date'::text THEN pa."FixedStartDate"
            ELSE NULL::timestamp without time zone
        END AS pass_valid_startdate,
    pa."FixedEndDate" AS pass_valid_enddate,
    tr."PassAtEntryDailyTripNum" AS pass_valid_trip_counter,
    tr."PassAtEntryTripsRemaining" AS pass_remain_trip,
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
     LEFT JOIN ( SELECT pr1."ValidatorUnconfirmedCommit",
            pr1."Self"
           FROM "Presentations" pr1
          WHERE "substring"(pr1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(pr2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Presentations" pr2
                  WHERE pr1."Self" = pr2."Self"))) pr_exit ON tr."ExitHref" = pr_exit."Self"
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
     JOIN ( SELECT pa1."TransferredDirection",
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
                  WHERE pa1."Self" = pa2."Self" AND pa2."State" = 'activated'::text))) pa ON tr."PassAtEntryHref" = pa."Self"
  WHERE (tr."TripState" = ANY (ARRAY['fare_paid'::text, 'fare_not_paid'::text])) AND pr."ValidatorPassengerDirection" = 'entering'::text AND pr."UpdatedTripHref" IS NOT NULL AND get_td_generate('STP ENTRY'::text, pr."LocationId", 0) = 1 AND pr."DeviceEquipmentName" IS NOT NULL AND pr."DeviceEquipmentShortName" IS NOT NULL AND pr."DeviceId" IS NOT NULL;


GRANT SELECT, TRUNCATE, UPDATE, DELETE, REFERENCES, TRIGGER, INSERT ON v_td_stp_entry_offline_replace_recovery TO cloudid;

COMMENT ON VIEW v_td_stp_entry_offline_replace_recovery IS 'Version: Version: 201905211640
Change log
201905211640: CLOUDID-695: change transaction_id 23 to 152 and change pass_status and change offline_replace_recovery Y to 1
201902081200: Change concept condition of TD generator with Pass
201901291230: Convert MessageId to bigint type
201901251000: Add >SELF< column by request
201901161000: Performance tuning query: Replace >SELECT DISTINCT ON< to >MessageId<
201809031600: CLOUDID-568 initial version 
201810301700: Change PassIdentificationNumber to IssuedAsPassIdentificationNumber support replacement
201811091500: Fixed replacement card when IssueAtPass is null
';
COMMIT;
