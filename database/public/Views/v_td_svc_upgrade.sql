DROP VIEW IF EXISTS v_td_svc_upgrade CASCADE;

CREATE OR REPLACE VIEW v_td_svc_upgrade
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
  upgrade_reason,
  penalty_amount,
  penalty_charged,
  cash_amount,
  bonus_amount,
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
  means,
  self,
  operator_id
)
AS 
 SELECT 240 AS message_type,
    1 AS payload_version_number,
    0 AS message_sequence_number,
    pr."DeviceId" AS bss_equipment_id,
    90 AS length_of_message,
    157 AS transaction_id,
    pr."ServiceProviderTimestamp" AS txn_datetime,
    1 AS service_provider_id,
    0 AS unconfirmed_indicator,
    1 AS project_id,
    "substring"(tk."CardId", 1, 3) AS issuer_id,
    tk."CardId" AS card_id,
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
    COALESCE(tp."Fee", 0) AS transaction_value,
    pr."ServiceProviderOperatingDate" AS operating_day,
    pr."DeviceEquipmentName" AS equipment_no,
        CASE
            WHEN tu."UpgradeType" = 'exit_mismatch'::text THEN 5
            WHEN tu."UpgradeType" = 'entry_mismatch'::text THEN 4
            WHEN tu."UpgradeType" = 'excess_time'::text THEN 6
            ELSE 0
        END AS upgrade_reason,
        CASE
            WHEN tu."UpgradeType" = 'entry_mismatch'::text THEN 0
            WHEN tu."UpgradeType" = 'exit_mismatch'::text THEN 0
            ELSE COALESCE(tp."Fee", 0)
        END AS penalty_amount,
    0 AS penalty_charged,
    COALESCE(tp."Fee", 0) AS cash_amount,
    0 AS bonus_amount,
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
    max(tr."SessionId") AS ssid,
    tp."Means" AS means,
    tr."Self" AS self,
    COALESCE(pr."OperatorId", 0::text) AS operator_id
   FROM "Trips" tr
     JOIN ( SELECT DISTINCT ON ("TripUpgrades"."TripReportSelf", "TripUpgrades"."Href") "TripUpgrades"."Href",
            "TripUpgrades"."TripReportSelf",
            "TripUpgrades"."Id",
            "TripUpgrades"."UpgradeType"
           FROM "TripUpgrades"
          WHERE "left"("TripUpgrades"."TripReportMessageId", 12) = 'Trip Payment'::text
          ORDER BY "TripUpgrades"."TripReportSelf", "TripUpgrades"."Href") tu ON tr."Self" = tu."TripReportSelf"
     JOIN "Presentations" pr ON tu."Href" = pr."Self"
     JOIN "TripUpgradePayments" tp ON tu."Id" = tp."TripUpgradeReportId"
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
  WHERE tp."Fee" IS NOT NULL
  GROUP BY pr."DeviceId", pr."ServiceProviderTimestamp", tk."CardId", tk."CardTypeId", tk."CardLifecycle", tp."Fee", pr."ServiceProviderOperatingDate", pr."DeviceEquipmentName", tu."UpgradeType", pr."LocationId", pr."DeviceEquipmentShortName", pr."LocationArrayNumber", tp."Means", tr."Self", pr."OperatorId";


GRANT SELECT, TRUNCATE, UPDATE, DELETE, REFERENCES, TRIGGER, INSERT ON v_td_svc_upgrade TO cloudid;

COMMENT ON VIEW v_td_svc_upgrade IS 'Version: 201905221200
Change log
201905221200: CLOUDID-695: add COALESCE(pr."OperatorId",0::text)::text
201905211640: CLOUDID-695: change transaction_id 45 to 157 and add operator_id
201903081000: Check TripUpgrade state to Payment Required
201901291230: Convert MessageId to bigint type
201901251000: Add >SELF< column by request
201901161000: Performance tuning query: Replace >SELECT DISTINCT ON< to >MessageId<
201803071200: update temporary solution  
201803061200: CLOUDIDTST-137
201802261900: fixed check state
201802261200: CLOUDIDTST-102
201802091200:  CLOUDIDTST-79';
COMMIT;
