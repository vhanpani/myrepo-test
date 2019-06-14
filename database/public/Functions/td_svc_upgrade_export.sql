CREATE OR REPLACE FUNCTION public.td_svc_upgrade_export()
  RETURNS SETOF text
  LANGUAGE sql
AS
$body$
-- update to processing only 4 row per request

SELECT public.td_flag_export('td_svc_upgrade') ; 

SELECT 'message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, transaction_date_and_time, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle_counter, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalisation_flag, transaction_value, operating_day, equipment_number, upgrade_reason, penalty_charged, by_ewallet, by_cash, by_credit_card, operator_id, transaction_mac_version, transaction_mac_value, bts_extended_data_format_version_number, bts_extension_data_ar_sequence_number, bts_extension_data_device_message_sequence_number, bts_extension_data_station_id, bts_extension_data_equipment_id, bts_extension_data_equipment_number, bts_extension_data_array_number, bts_extension_data_cloud_id_ar_sequence_number, bts_extension_data_cloud_id_device_message_sequence_number, bts_ext_spare'

UNION ALL 

   
select coalesce(message_type::text,'0') 
|| ', ' ||    coalesce(payload_version_number::text,'0')
|| ', ' ||    coalesce(message_sequence_number::text,'0') 
|| ', ' ||    hex_to_int(coalesce(bss_equipment_id::text,'0')) 
|| ', ' ||    coalesce(length_of_message::text,'0') 
|| ', ' ||    coalesce(transaction_id::text,'0') 
|| ', ' ||    coalesce((txn_datetime+ interval '7 hours' )::text,'0') 
|| ', ' ||    coalesce(service_provider_id::text,'0') 
|| ', ' ||    coalesce(unconfirmed_indicator::text,'0') 
|| ', ' ||    coalesce(project_id::text,'0') 
|| ', ' ||    coalesce(issuer_id::text,'0') 
|| ', ' ||    coalesce(card_id::text,'0') 
|| ', ' ||    coalesce(card_type::text,'0') 
|| ', ' ||    coalesce(lifecycle::text,'0') 
|| ', ' ||    coalesce(card_status::text,'0') 
|| ', ' ||    coalesce(block_flag::text,'0') 
|| ', ' ||    coalesce(test_flag::text,'0') 
|| ', ' ||    coalesce(card_sequence_number::text,'0') 
|| ', ' ||    coalesce(card_unblock_sequence_number::text,'0') 
|| ', ' ||    coalesce(personalization_flag::text,'0') 
|| ', ' ||    coalesce(transaction_value::text,'0') 
|| ', ' ||    coalesce(operating_day::text,'0') 
|| ', ' ||    coalesce(equipment_no::text,'0') 
|| ', ' ||    coalesce(upgrade_reason::text,'0') 
|| ', ' ||    coalesce(penalty_charged::text,'0')
|| ', ' ||    coalesce(penalty_amount::text,'0')
|| ', ' ||    coalesce(cash_amount::text,'0')
|| ', ' ||    coalesce(bonus_amount::text,'0')
|| ', ' ||    coalesce(operator_id::text,'0') 
|| ', ' ||    coalesce(trans_mac_version::text,'0') 
|| ', ' ||    coalesce(trans_mac_value::text,'0') 
|| ', ' ||    coalesce(bts_ext_format_version_number::text,'0') 
|| ', ' ||    coalesce(bts_ext_ar_seq_num::text,'0') 
|| ', ' ||    coalesce(bts_ext_device_msg_seq_num::text,'0') 
|| ', ' ||    coalesce(bts_ext_station_id::text,'0') 
|| ', ' ||    coalesce(bts_ext_equipment_id::text,'0') 
|| ', ' ||    coalesce(bts_ext_equipment_number::text,'0') 
|| ', ' ||    coalesce(bts_ext_array_number::text,'0') 
|| ', ' ||    coalesce(bts_ext_cloud_id_ar_seq::text,'0') 
|| ', ' ||    coalesce(bts_ext_cloud_id_dev_msg_seq::text,'0') 
|| ', ' ||    coalesce(bts_ext_spare::text,'0') 
from public.td_svc_upgrade  
where ssid <=  (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_svc_upgrade' and ( reported = 'P' )) 
and ssid > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report  = 'td_svc_upgrade' and ( reported  = 'Y' ));
$body$
  VOLATILE
  COST 100
  ROWS 1000;

COMMENT ON FUNCTION td_svc_upgrade_export() IS 'Version: 201905231111 
	201905231111 : CLOUDID-695: add column operator_id
	201803221500 : update to +7h to timestamp
	201801291500 : initial vesion';


COMMIT;
