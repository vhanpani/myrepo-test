DROP VIEW IF EXISTS v_td_stp_transfer CASCADE;

CREATE OR REPLACE VIEW v_td_stp_transfer
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
  new_card_project_id,
  new_card_issuer_id,
  new_card_card_id,
  new_card_card_type,
  new_card_lifecycle_counter,
  new_card_card_status,
  new_card_block_flag,
  new_card_test_flag,
  new_card_card_sequence_number,
  new_card_card_unblock_sequence_number,
  new_card_personalisation_flag,
  old_card_project_id,
  old_card_issuer_id,
  old_card_card_id,
  old_card_card_type,
  old_card_lifecycle_counter,
  old_card_card_status,
  old_card_block_flag,
  old_card_test_flag,
  old_card_card_sequence_number,
  old_card_card_unblock_sequence_number,
  old_card_personalisation_flag,
  pass_1_pass_identification_number,
  pass_1_sp_id_of_pass_issuer,
  pass_1_pass_status,
  pass_1_pass_type_id,
  pass_1_pass_valid_start_date,
  pass_1_pass_valid_end_date,
  pass_1_pass_daily_trip_counter,
  pass_1_pass_remaining_trip_counter,
  pass_2_pass_identification_number,
  pass_2_sp_id_of_pass_issuer,
  pass_2_pass_status,
  pass_2_pass_type_id,
  pass_2_pass_valid_start_date,
  pass_2_pass_valid_end_date,
  pass_2_pass_daily_trip_counter,
  pass_2_pass_remaining_trip_counter,
  stp_1_pass_price,
  stp_1_pass_validity_duration_mode,
  stp_1_pass_first_use_expiry_date,
  stp_1_pass_fixed_start_date,
  stp_1_pass_fixed_end_date,
  stp_1_pass_validity_duration,
  stp_1_number_of_trips_issued,
  stp_1_pass_average_trip_fare,
  stp_1_pass_daily_trip_limit,
  stp_1_pass_bonus_time_added,
  stp_1_pass_slot_no,
  stp_2_pass_price,
  stp_2_pass_validity_duration_mode,
  stp_2_pass_first_use_expiry_date,
  stp_2_pass_fixed_start_date,
  stp_2_pass_fixed_end_date,
  stp_2_pass_validity_duration,
  stp_2_number_of_trips_issued,
  stp_2_pass_average_trip_fare,
  stp_2_pass_daily_trip_limit,
  stp_2_pass_bonus_time_added,
  stp_2_pass_slot_no,
  operating_day,
  equipment_number,
  operator_id,
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
  self
)
AS 
 SELECT 240 AS message_type,
    1 AS payload_version_number,
    0 AS message_sequence_number,
    '0'::text AS bss_equipment_id,
    170 AS length_of_message,
    160 AS transaction_id,
    tf."ReportedTimestamp" AS transaction_date_and_time,
    1 AS service_provider_id,
    0 AS unconfirmed_indicator,
    0 AS new_card_project_id,
    "substring"(tk_new."CardId", 1, 3) AS new_card_issuer_id,
    tk_new."CardId" AS new_card_card_id,
    tk_new."CardTypeId" AS new_card_card_type,
    tk_new."CardLifecycle" AS new_card_lifecycle_counter,
    0 AS new_card_card_status,
    0 AS new_card_block_flag,
    0 AS new_card_test_flag,
    0 AS new_card_card_sequence_number,
    0 AS new_card_card_unblock_sequence_number,
    0 AS new_card_personalisation_flag,
    0 AS old_card_project_id,
    "substring"(tk_old."CardId", 1, 3) AS old_card_issuer_id,
    tk_old."CardId" AS old_card_card_id,
    tk_old."CardTypeId" AS old_card_card_type,
    tk_old."CardLifecycle" AS old_card_lifecycle_counter,
    0 AS old_card_card_status,
    0 AS old_card_block_flag,
    0 AS old_card_test_flag,
    0 AS old_card_card_sequence_number,
    0 AS old_card_card_unblock_sequence_number,
    0 AS old_card_personalisation_flag,
        CASE
            WHEN pa_old1."PassIdentificationNumber" <> pa_old1."IssuedAsPassIdentificationNumber" THEN pa_old1."IssuedAsPassIdentificationNumber"
            ELSE pa_old1."PassIdentificationNumber"
        END AS pass_1_pass_identification_number,
    1 AS pass_1_sp_id_of_pass_issuer,
        CASE pa_old1."State"
            WHEN 'issued'::text THEN 1
            WHEN 'activated'::text THEN 2
            WHEN 'expired'::text THEN 3
            WHEN 'reversed'::text THEN 4
            WHEN 'exhausted'::text THEN 5
            WHEN 'transferred'::text THEN 6
            WHEN 'canceled'::text THEN 7
            ELSE NULL::integer
        END AS pass_1_pass_status,
    pa_old1."PassType" AS pass_1_pass_type_id,
        CASE
            WHEN pa_old1."ValidityDurationMode" = 'from_issue'::text THEN pa_old1."OperatingDay"::timestamp without time zone
            WHEN pa_old1."ValidityDurationMode" = 'from_first_use'::text THEN NULL::timestamp without time zone
            WHEN pa_old1."ValidityDurationMode" = 'fixed_date'::text THEN pa_old1."FixedStartDate"
            ELSE NULL::timestamp without time zone
        END AS pass_1_pass_valid_start_date,
        CASE
            WHEN pa_old1."ValidityDurationMode" = 'from_issue'::text THEN pa_old1."FixedEndDate"
            WHEN pa_old1."ValidityDurationMode" = 'fixed_date'::text THEN pa_old1."FixedEndDate"
            ELSE NULL::timestamp without time zone
        END AS pass_1_pass_valid_end_date,
    pa_old1."NumDailyTrips" AS pass_1_pass_daily_trip_counter,
    COALESCE(pa_old1."MaxTrips", 0) - COALESCE(pa_old1."NumTrips", 0) AS pass_1_pass_remaining_trip_counter,
        CASE
            WHEN tf.cnttransfer = 2 THEN
            CASE
                WHEN pa_old2."PassIdentificationNumber" <> pa_old2."IssuedAsPassIdentificationNumber" THEN pa_old2."IssuedAsPassIdentificationNumber"
                ELSE pa_old2."PassIdentificationNumber"
            END
            ELSE NULL::oid
        END AS pass_2_pass_identification_number,
        CASE
            WHEN tf.cnttransfer = 2 THEN 1
            ELSE NULL::integer
        END AS pass_2_sp_id_of_pass_issuer,
        CASE
            WHEN tf.cnttransfer = 2 THEN
            CASE pa_old2."State"
                WHEN 'issued'::text THEN 1
                WHEN 'activated'::text THEN 2
                WHEN 'expired'::text THEN 3
                WHEN 'reversed'::text THEN 4
                WHEN 'exhausted'::text THEN 5
                WHEN 'transferred'::text THEN 6
                WHEN 'canceled'::text THEN 7
                ELSE NULL::integer
            END
            ELSE NULL::integer
        END AS pass_2_pass_status,
        CASE
            WHEN tf.cnttransfer = 2 THEN pa_old2."PassType"
            ELSE NULL::text
        END AS pass_2_pass_type_id,
        CASE
            WHEN tf.cnttransfer = 2 THEN
            CASE
                WHEN pa_old2."ValidityDurationMode" = 'from_issue'::text THEN pa_old2."OperatingDay"::timestamp without time zone
                WHEN pa_old2."ValidityDurationMode" = 'from_first_use'::text THEN NULL::timestamp without time zone
                WHEN pa_old2."ValidityDurationMode" = 'fixed_date'::text THEN pa_old2."FixedStartDate"
                ELSE NULL::timestamp without time zone
            END
            ELSE NULL::timestamp without time zone
        END AS pass_2_pass_valid_start_date,
        CASE
            WHEN tf.cnttransfer = 2 THEN
            CASE
                WHEN pa_old2."ValidityDurationMode" = 'from_issue'::text THEN pa_old2."FixedEndDate"
                WHEN pa_old2."ValidityDurationMode" = 'fixed_date'::text THEN pa_old2."FixedEndDate"
                ELSE NULL::timestamp without time zone
            END
            ELSE NULL::timestamp without time zone
        END AS pass_2_pass_valid_end_date,
        CASE
            WHEN tf.cnttransfer = 2 THEN pa_old2."NumDailyTrips"
            ELSE NULL::integer
        END AS pass_2_pass_daily_trip_counter,
        CASE
            WHEN tf.cnttransfer = 2 THEN COALESCE(pa_old2."MaxTrips", 0) - COALESCE(pa_old2."NumTrips", 0)
            ELSE NULL::integer
        END AS pass_2_pass_remaining_trip_counter,
        CASE
            WHEN tf.cnttransfer = 2 THEN COALESCE(pa_old1."Price"::numeric, 0.00) * 100::numeric
            ELSE NULL::integer::numeric
        END AS stp_1_pass_price,
    pa_old1."ValidityDurationMode" AS stp_1_pass_validity_duration_mode,
    pa_old1."FirstUseExpiryDate" AS stp_1_pass_first_use_expiry_date,
    pa_old1."FixedStartDate" AS stp_1_pass_fixed_start_date,
    pa_old1."FixedEndDate" AS stp_1_pass_fixed_end_date,
    pa_old1."ValidityDuration" AS stp_1_pass_validity_duration,
    pa_old1."MaxTrips" AS stp_1_number_of_trips_issued,
    pa_old1."AverageTripFare" AS stp_1_pass_average_trip_fare,
    pa_old1."MaxDailyTrips" AS stp_1_pass_daily_trip_limit,
    0 AS stp_1_pass_bonus_time_added,
    2 AS stp_1_pass_slot_no,
        CASE
            WHEN tf.cnttransfer = 2 THEN COALESCE(pa_old2."Price"::numeric, 0.00) * 100::numeric
            ELSE NULL::numeric
        END AS stp_2_pass_price,
        CASE
            WHEN tf.cnttransfer = 2 THEN pa_old2."ValidityDurationMode"
            ELSE NULL::text
        END AS stp_2_pass_validity_duration_mode,
        CASE
            WHEN tf.cnttransfer = 2 THEN pa_old2."FirstUseExpiryDate"
            ELSE NULL::timestamp without time zone
        END AS stp_2_pass_first_use_expiry_date,
        CASE
            WHEN tf.cnttransfer = 2 THEN pa_old2."FixedStartDate"
            ELSE NULL::timestamp without time zone
        END AS stp_2_pass_fixed_start_date,
        CASE
            WHEN tf.cnttransfer = 2 THEN pa_old2."FixedEndDate"
            ELSE NULL::timestamp without time zone
        END AS stp_2_pass_fixed_end_date,
        CASE
            WHEN tf.cnttransfer = 2 THEN pa_old2."ValidityDuration"
            ELSE NULL::integer
        END AS stp_2_pass_validity_duration,
        CASE
            WHEN tf.cnttransfer = 2 THEN pa_old2."MaxTrips"
            ELSE NULL::integer
        END AS stp_2_number_of_trips_issued,
        CASE
            WHEN tf.cnttransfer = 2 THEN pa_old2."AverageTripFare"
            ELSE NULL::integer
        END AS stp_2_pass_average_trip_fare,
        CASE
            WHEN tf.cnttransfer = 2 THEN pa_old2."MaxDailyTrips"
            ELSE NULL::integer
        END AS stp_2_pass_daily_trip_limit,
        CASE
            WHEN tf.cnttransfer = 2 THEN 0
            ELSE NULL::integer
        END AS stp_2_pass_bonus_time_added,
        CASE
            WHEN tf.cnttransfer = 2 THEN 2
            ELSE NULL::integer
        END AS stp_2_pass_slot_no,
    pa_old1."OperatingDay" AS operating_day,
    pa_old1."DeviceEquipmentName" AS equipment_number,
    COALESCE(pa_old1."DeviceOperatorId", 0::text) AS operator_id,
    0 AS transaction_mac_version,
    0 AS transaction_mac_value,
    1 AS bts_ext_format_version_number,
    0 AS bts_ext_ar_seq_num,
    0 AS bts_ext_device_msg_seq_num,
    127 AS bts_ext_station_id,
    pa_old1."DeviceEquipmentName" AS bts_ext_equipment_id,
    pa_old1."DeviceEquipmentShortName" AS bts_ext_equipment_number,
    1 AS bts_ext_array_number,
    0 AS bts_ext_cloud_id_ar_seq,
    0::bigint AS bts_ext_cloud_id_dev_msg_seq,
    0 AS bts_ext_spare,
    tf."SessionId" AS ssid,
    tf."Self" AS self
   FROM ( SELECT tkp."SessionId",
            tkp."ReportedTimestamp",
            tkp."TokenHref",
            tkp."ReplacedTokenHref",
            tkp."Self",
            ( SELECT count(*) AS count
                   FROM "Passes" pa
                  WHERE tkp."ReplacedTokenHref" = pa."Token"::text AND tkp."TokenHref" = pa."TransferredTokenHref" AND tkp."SessionId" = pa."SessionId" AND pa."TransferredDirection" = 'to'::text AND pa."State" = 'transferred'::text AND tkp."ReportedTimestamp" >= (pa."ReportedTimestamp" + '-00:00:30'::interval second) AND tkp."ReportedTimestamp" <= pa."ReportedTimestamp") AS cnttransfer
           FROM "TokenReplacements" tkp
          WHERE tkp."Direction" = 'from'::text) tf
     JOIN ( SELECT tk1."CardId",
            tk1."CardTypeId",
            tk1."CardLifecycle",
            tk1."Self"
           FROM "Tokens" tk1
          WHERE (tk1."State" = 'bound'::text OR tk1."State" = 'assigned'::text) AND "substring"(tk1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(tk2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Tokens" tk2
                  WHERE tk1."Self" = tk2."Self" AND (tk2."State" = 'bound'::text OR tk2."State" = 'assigned'::text)))) tk_new ON tf."TokenHref" = tk_new."Self"
     JOIN ( SELECT tk1."CardId",
            tk1."CardTypeId",
            tk1."CardLifecycle",
            tk1."Self"
           FROM "Tokens" tk1
          WHERE tk1."State" = 'bound'::text AND "substring"(tk1."MessageId", '.*/.*/(.*)'::text)::bigint = (( SELECT max("substring"(tk2."MessageId", '.*/.*/(.*)'::text)::bigint) AS "MessageId"
                   FROM "Tokens" tk2
                  WHERE tk1."Self" = tk2."Self" AND tk2."State" = 'bound'::text))) tk_old ON tf."ReplacedTokenHref" = tk_old."Self"
     LEFT JOIN ( SELECT pa."TransferredTokenHref",
            pa."SessionId",
            pa."Token",
            pa."PassIdentificationNumber",
            pa."IssuedAsPassIdentificationNumber",
            pa."State",
            pa."PassType",
            pa."FixedStartDate",
            pa."FixedEndDate",
            pa."NumDailyTrips",
            pa."MaxTrips",
            pa."NumTrips",
            pa."Price",
            pa."ValidityDurationMode",
            pa."FirstUseExpiryDate",
            pa."ValidityDuration",
            pa."AverageTripFare",
            pa."MaxDailyTrips",
            pa."OperatingDay",
            pa."DeviceOperatorId",
            pa."DeviceEquipmentName",
            pa."DeviceEquipmentShortName",
            pa."ReportedTimestamp"
           FROM "Passes" pa
          WHERE pa."TransferredDirection" = 'from'::text AND pa."TimeOfIssue" = (( SELECT min(pa2."TimeOfIssue") AS min
                   FROM "Passes" pa2
                  WHERE pa2."TransferredDirection" = 'from'::text AND pa."Token"::text = pa2."Token"::text AND pa."SessionId" = pa2."SessionId"))) pa_old1 ON tf."TokenHref" = pa_old1."Token"::text AND tf."ReplacedTokenHref" = pa_old1."TransferredTokenHref" AND tf."SessionId" = pa_old1."SessionId" AND tf."ReportedTimestamp" >= (pa_old1."ReportedTimestamp" + '-00:00:30'::interval second) AND tf."ReportedTimestamp" <= pa_old1."ReportedTimestamp"
     LEFT JOIN ( SELECT pa."TransferredTokenHref",
            pa."SessionId",
            pa."Token",
            pa."PassIdentificationNumber",
            pa."IssuedAsPassIdentificationNumber",
            pa."State",
            pa."PassType",
            pa."FixedStartDate",
            pa."FixedEndDate",
            pa."NumDailyTrips",
            pa."MaxTrips",
            pa."NumTrips",
            pa."Price",
            pa."ValidityDurationMode",
            pa."FirstUseExpiryDate",
            pa."ValidityDuration",
            pa."AverageTripFare",
            pa."MaxDailyTrips",
            pa."OperatingDay",
            pa."DeviceOperatorId",
            pa."DeviceEquipmentName",
            pa."DeviceEquipmentShortName",
            pa."ReportedTimestamp"
           FROM "Passes" pa
          WHERE pa."TransferredDirection" = 'from'::text AND pa."TimeOfIssue" = (( SELECT max(pa2."TimeOfIssue") AS max
                   FROM "Passes" pa2
                  WHERE pa2."TransferredDirection" = 'from'::text AND pa."Token"::text = pa2."Token"::text AND pa."SessionId" = pa2."SessionId"))) pa_old2 ON tf."TokenHref" = pa_old2."Token"::text AND tf."ReplacedTokenHref" = pa_old2."TransferredTokenHref" AND tf."SessionId" = pa_old2."SessionId" AND tf."ReportedTimestamp" >= (pa_old2."ReportedTimestamp" + '-00:00:30'::interval second) AND tf."ReportedTimestamp" <= pa_old2."ReportedTimestamp"
  WHERE tf.cnttransfer > 0
  ORDER BY tf."ReportedTimestamp";


GRANT SELECT, TRUNCATE, UPDATE, DELETE, REFERENCES, TRIGGER, INSERT ON v_td_stp_transfer TO cloudid;

COMMENT ON VIEW v_td_stp_transfer IS 'Version: 201905291814
Change log
201905291814:CLOUDID-698 NEW TD v_td_stp_transfer
';
COMMIT;
