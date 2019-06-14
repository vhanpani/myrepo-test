DROP VIEW IF EXISTS v_td_stp_issue CASCADE;

CREATE OR REPLACE VIEW v_td_stp_issue
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
  payment_mean_type,
  payment_amount1_val,
  payment_amount1_num,
  payment_amount2_val,
  payment_amount2_num,
  payment_amount3_val,
  payment_amount3_num,
  pin,
  sp_id,
  pass_status,
  passtype,
  pass_valid_startdate,
  pass_valid_enddate,
  pass_valid_trip_counter,
  pass_remain_trip,
  pass_price,
  validity_duration_mode,
  pass_first_use_expire_date,
  pass_fixed_start_date,
  pass_fixed_end_date,
  pass_validity_duration,
  pass_num_of_trip_issued,
  pass_average_trip_fare,
  pass_daily_trip_limit,
  pass_bonus,
  pass_slotno,
  operating_day,
  equipment_no,
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
  self,
  payment_means_4_amount_received,
  payment_means_4_identification_number,
  operator_id
)
AS 
 SELECT DISTINCT ON (pa."Self") 240 AS message_type,
    1 AS payload_version_number,
    0 AS message_sequence_number,
    '0'::text AS bss_equipment_id,
    160 AS length_of_message,
    155 AS transaction_id,
    pa."TimeOfIssue" AS txn_datetime,
    1 AS service_provider_id,
    0 AS unconfirmed_indicator,
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
    0 AS lav_service_provider_id,
    0 AS lav_location_code,
    NULL::timestamp without time zone AS lav_txn_datetime,
    0 AS lav_amount,
    0 AS lav_payment,
    0 AS lav_equipmentid,
    0 AS lav_csn,
    COALESCE(pa."Price"::numeric, 0.00) * 100::numeric AS transaction_value,
    11 AS payment_mean_type,
    0 AS payment_amount1_val,
    0 AS payment_amount1_num,
    0 AS payment_amount2_val,
    0 AS payment_amount2_num,
    COALESCE(pa."Price"::numeric, 0.00) * 100::numeric AS payment_amount3_val,
    0 AS payment_amount3_num,
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
            WHEN pa."ValidityDurationMode" = 'from_first_use'::text THEN NULL::timestamp without time zone
            WHEN pa."ValidityDurationMode" = 'fixed_date'::text THEN pa."FixedStartDate"
            ELSE NULL::timestamp without time zone
        END AS pass_valid_startdate,
        CASE
            WHEN pa."ValidityDurationMode" = 'from_issue'::text THEN pa."FixedEndDate"
            WHEN pa."ValidityDurationMode" = 'fixed_date'::text THEN pa."FixedEndDate"
            ELSE NULL::timestamp without time zone
        END AS pass_valid_enddate,
    pa."NumDailyTrips" AS pass_valid_trip_counter,
    COALESCE(pa."MaxTrips", 0) - COALESCE(pa."NumTrips", 0) AS pass_remain_trip,
    COALESCE(pa."Price"::numeric, 0.00) * 100::numeric AS pass_price,
    pa."ValidityDurationMode" AS validity_duration_mode,
    pa."FirstUseExpiryDate" AS pass_first_use_expire_date,
    pa."FixedStartDate" AS pass_fixed_start_date,
    pa."FixedEndDate" AS pass_fixed_end_date,
    pa."ValidityDuration" AS pass_validity_duration,
    pa."MaxTrips" AS pass_num_of_trip_issued,
    pa."AverageTripFare" AS pass_average_trip_fare,
    pa."MaxDailyTrips" AS pass_daily_trip_limit,
    0 AS pass_bonus,
    2 AS pass_slotno,
    pa."OperatingDay" AS operating_day,
    pa."DeviceEquipmentName" AS equipment_no,
    0 AS trans_mac_version,
    0 AS trans_mac_value,
    1 AS bts_ext_format_version_number,
    0 AS bts_ext_ar_seq_num,
    0 AS bts_ext_device_msg_seq_num,
    127 AS bts_ext_station_id,
    pa."DeviceEquipmentName" AS bts_ext_equipment_id,
    pa."DeviceEquipmentShortName" AS bts_ext_equipment_number,
    1 AS bts_ext_array_number,
    0 AS bts_ext_cloud_id_ar_seq,
    0::bigint AS bts_ext_cloud_id_dev_msg_seq,
    0 AS bts_ext_spare,
    pa."SessionId" AS ssid,
    pa."Self" AS self,
    0 AS payment_means_4_amount_received,
    0 AS payment_means_4_identification_number,
    COALESCE(pa."DeviceOperatorId", 0::text) AS operator_id
   FROM "Passes" pa
     JOIN ( SELECT tk1."Self",
            tk1."CardId",
            tk1."CardTypeId",
            tk1."CardLifecycle"
           FROM "Tokens" tk1
          WHERE tk1."State" = 'bound'::text AND "substring"(tk1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(tk2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Tokens" tk2
                  WHERE tk1."Self" = tk2."Self" AND tk2."State" = 'bound'::text))) tk ON pa."Token"::text = tk."Self"
  WHERE pa."State" = 'issued'::text AND pa."IssuedAsPassIdentificationNumber" = pa."PassIdentificationNumber" OR pa."State" = 'activated'::text AND pa."ValidityDurationMode" = 'from_issue'::text
  ORDER BY pa."Self", pa."ReportedTimestamp";


GRANT SELECT, TRUNCATE, UPDATE, DELETE, REFERENCES, TRIGGER, INSERT ON v_td_stp_issue TO cloudid;

COMMENT ON VIEW v_td_stp_issue IS 'Version: 201905221200
Change log
201905221200: CLOUDID-695: add operator_id = COALESCE(pa."DeviceOperatorId",0::text)::text and change equipment_no,bts_ext_equipment_id,bts_ext_equipment_number
201905211640: CLOUDID-695: change transaction_id 14 to 155 and change pass_status and add payment_means_4_amount_received,payment_means_4_identification_number,operator_id
201901291230: Convert MessageId to bigint type
201901251000: Add >SELF< column by request
201901161000: Performance tuning query: Replace >SELECT DISTINCT ON< to >MessageId<
201804021200: CLOUDIDTST-273
201803151200: CLOUDIDTST-220
201802281900: CLOUDIDTST-129
201802191900: CLOUDIDTST-82
201802091200: CLOUDIDTST-23, CLOUDIDTST-79
2018-02-06: Fixed CLOUDIDTST-23
2018-02-05: Fixed CLOUDIDTST-77
2018-01-29: Fixed CLOUDIDTST-61
201810301700: Change PassIdentificationNumber to IssuedAsPassIdentificationNumber support replacement
201811091500: Fixed replacement card when IssueAtPass is null
';
COMMIT;
