DROP VIEW IF EXISTS v_td_stp_issue_reversal CASCADE;

CREATE OR REPLACE VIEW v_td_stp_issue_reversal
(
  message_type,
  payload_version_number,
  message_sequence_number,
  bss_equipment_id,
  length_of_message,
  transaction_id,
  transaction_date_and_time,
  service_provider_id,
  unconfirmed_indicator,
  project_id,
  issuer_id,
  card_id,
  card_type,
  lifecycle_counter,
  card_status,
  block_flag,
  test_flag,
  card_sequence_number,
  card_unblock_sequence_number,
  personalization_flag,
  transaction_value,
  pass_identification_number,
  sp_id_of_pass_issuer,
  pass_status,
  product_type_id,
  pass_valid_start_date,
  pass_valid_end_date,
  pass_daily_trip_counter,
  pass_remaining_trip_counter,
  pass_price,
  pass_validity_duration_mode,
  pass_first_use_expiry_date,
  pass_fixed_start_date,
  pass_fixed_end_date,
  pass_validity_duration,
  pass_number_of_trip_issued,
  pass_average_trip_fare,
  pass_daily_trip_limit,
  pass_bonus_time_added,
  pass_slot_no,
  operating_day,
  equipment_number,
  transaction_mac_version,
  transaction_mac_value,
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
  self,
  operator_id
)
AS 
 SELECT 240 AS message_type,
    1 AS payload_version_number,
    0 AS message_sequence_number,
    '0'::text AS bss_equipment_id,
    160 AS length_of_message,
    156 AS transaction_id,
    prvs."ServiceTimestamp" AS transaction_date_and_time,
    1 AS service_provider_id,
    0 AS unconfirmed_indicator,
    1 AS project_id,
    "substring"(tk."CardId", 1, 3) AS issuer_id,
        CASE
            WHEN COALESCE(pa."TransferredDirection", 'N'::text) = 'from'::text THEN ( SELECT tk3."CardId" AS "substring"
               FROM "Tokens" tk3
              WHERE tk3."Self" = COALESCE(pa."IssuedAsTokenHref", pa."TransferredTokenHref")
              GROUP BY tk3."CardId"
             LIMIT 1)
            ELSE tk."CardId"
        END AS card_id,
    tk."CardTypeId" AS card_type,
    tk."CardLifecycle" AS lifecycle_counter,
    0 AS card_status,
    0 AS block_flag,
    0 AS test_flag,
    0 AS card_sequence_number,
    0 AS card_unblock_sequence_number,
    0 AS personalization_flag,
    pa."Price" AS transaction_value,
    pa."PassIdentificationNumber" AS pass_identification_number,
    1 AS sp_id_of_pass_issuer,
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
    pa."PassType" AS product_type_id,
    pa."FixedStartDate" AS pass_valid_start_date,
    pa."FixedEndDate" AS pass_valid_end_date,
    pa."NumDailyTrips" AS pass_daily_trip_counter,
    COALESCE(pa."MaxTrips", 0) - COALESCE(pa."NumTrips", 0) AS pass_remaining_trip_counter,
    pa."Price" AS pass_price,
    pa."ValidityDurationMode" AS pass_validity_duration_mode,
    pa."FirstUseExpiryDate" AS pass_first_use_expiry_date,
    pa."FixedStartDate" AS pass_fixed_start_date,
    pa."FixedEndDate" AS pass_fixed_end_date,
    pa."ValidityDuration" AS pass_validity_duration,
    pa."MaxTrips" AS pass_number_of_trip_issued,
    pa."AverageTripFare" AS pass_average_trip_fare,
    pa."MaxDailyTrips" AS pass_daily_trip_limit,
    0 AS pass_bonus_time_added,
    2 AS pass_slot_no,
    pa."OperatingDay" AS operating_day,
    tk."DeviceEquipmentName" AS equipment_number,
    0 AS transaction_mac_version,
    0 AS transaction_mac_value,
    1 AS bts_ext_format_version_number,
    0 AS bts_ext_ar_seq_num,
    0 AS bts_ext_device_msg_seq_num,
    127 AS bts_ext_station_id,
    prvs."DeviceEquipmentName" AS bts_ext_equipment_id,
    prvs."DeviceEquipmentShortName" AS bts_ext_equipment_number,
    1 AS bts_ext_array_number,
    0 AS bts_ext_cloud_id_ar_seq,
    0::bigint AS bts_ext_cloud_id_dev_msg_seq,
    0 AS bts_ext_spare,
    prvs."SessionId" AS ssid,
    prvs."Self" AS self,
    COALESCE(pa."DeviceOperatorId", '0'::text) AS operator_id
   FROM "PassReversals" prvs
     JOIN ( SELECT pa1."TransferredDirection",
            pa1."IssuedAsTokenHref",
            pa1."TransferredTokenHref",
            pa1."Price",
            pa1."PassIdentificationNumber",
            pa1."State",
            pa1."PassType",
            pa1."FixedStartDate",
            pa1."FixedEndDate",
            pa1."NumDailyTrips",
            pa1."MaxTrips",
            pa1."NumTrips",
            pa1."ValidityDurationMode",
            pa1."FirstUseExpiryDate",
            pa1."ValidityDuration",
            pa1."AverageTripFare",
            pa1."MaxDailyTrips",
            pa1."OperatingDay",
            pa1."Self",
            pa1."SessionId",
            pa1."Token",
            pa1."DeviceOperatorId"
           FROM "Passes" pa1
          WHERE pa1."State" = 'reversed'::text) pa ON prvs."PassHref" = pa."Self" AND prvs."SessionId" = pa."SessionId"
     JOIN ( SELECT tk1."Self",
            tk1."CardId",
            tk1."CardTypeId",
            tk1."CardLifecycle",
            tk1."DeviceEquipmentName"
           FROM "Tokens" tk1
          WHERE tk1."State" = 'bound'::text AND "substring"(tk1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(tk2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Tokens" tk2
                  WHERE tk1."Self" = tk2."Self" AND tk2."State" = 'bound'::text))) tk ON pa."Token"::text = tk."Self"
  ORDER BY prvs."Self", prvs."ReportedTimestamp";


GRANT SELECT, TRUNCATE, UPDATE, DELETE, REFERENCES, TRIGGER, INSERT ON v_td_stp_issue_reversal TO cloudid;

COMMENT ON VIEW v_td_stp_issue_reversal IS 'Version: 201905221601
Change log
201905221601:CLOUDID-696 NEW TD v_td_stp_issue_reversal
';
COMMIT;
