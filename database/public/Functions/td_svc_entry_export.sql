CREATE OR REPLACE FUNCTION public.td_svc_entry_export()
  RETURNS SETOF text
  LANGUAGE sql
AS
$body$
-- update to processing only 4 row per request

SELECT public.td_flag_export('td_svc_entry') ; 

SELECT 'message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, transaction_date_and_time, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle_counter, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalisation_flag, operating_day, equipment_number, entry_station_id, degrade_state, array_number, cloud_txn_status, transaction_mac_version, transaction_mac_value, bts_extended_data_format_version_number, bts_extension_data_ar_sequence_number, bts_extension_data_device_message_sequence_number, bts_extension_data_station_id, bts_extension_data_equipment_id, bts_extension_data_equipment_number, bts_extension_data_array_number, bts_extension_data_cloud_id_ar_sequence_number, bts_extension_data_cloud_id_device_message_sequence_number, bts_ext_spare'

UNION ALL 
 
select coalesce(message_type::text,'0') 
|| ', ' ||    coalesce(payload_version_number::text,'0')
|| ', ' ||    coalesce(message_sequence_number::text,'0') 
|| ', ' ||    hex_to_int(coalesce(bss_equipment_id::text,'0')) 
|| ', ' ||    coalesce(length_of_message::text,'0') 
|| ', ' ||    coalesce(transaction_id::text,'0') 
|| ', ' ||    coalesce((txn_datetime + interval '7 hours' )::text,'0') 
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
|| ', ' ||    coalesce(operating_day::text,'0') 
|| ', ' ||    coalesce(equipment_no::text,'0') 
|| ', ' ||    coalesce(entry_location::text,'0')
|| ', ' ||    coalesce(fare_mode::text,'0')
|| ', ' ||    coalesce(array_number::text,'0')
|| ', ' ||    coalesce(offline_replace_recovery::text,'0')
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
from public.td_svc_entry  
where
(
ssid <=  (SELECT coalesce(max(ssid), date '1970-01-01') FROM  public.td_ss_info WHERE report = 'td_svc_entry' and ( reported = 'P' )) 
and ssid > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  public.td_ss_info WHERE report  = 'td_svc_entry' and ( reported  = 'Y' ))
	AND  offline_replace_recovery = '0' --'N'	
)
OR
(
ssid <=  (SELECT coalesce(max(ssid), date '1970-01-01') FROM  public.td_ss_info WHERE report = 'td_svc_entry_offline_replace_recovery' and ( reported = 'P' )) 
and ssid > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  public.td_ss_info WHERE report  = 'td_svc_entry_offline_replace_recovery' and ( reported  = 'Y' ))
	AND  offline_replace_recovery = '1' --'Y'	
);
$body$
  VOLATILE
  COST 100
  ROWS 1000;

COMMENT ON FUNCTION public.td_svc_entry_export() IS 'Version: 201808231600
201808231600: CLOUDID-568
	201803221500 : update to +7h to timestamp
	201801291500 : initial vesion';


COMMIT;
