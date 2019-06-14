CREATE OR REPLACE FUNCTION public.td_stp_confiscate_export()
  RETURNS SETOF text
  LANGUAGE sql
AS
$body$
-- update to processing only 4 row per request

SELECT public.td_flag_export('td_stp_confiscate') ; 

SELECT 'message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, transaction_date_and_time, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle_counter, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalisation_flag, pass_identification_number, sp_id_of_pass_issuer, pass_status, product_type_id, pass_valid_start_date, pass_valid_end_date, pass_daily_trip_counter, pass_remaining_trip_counter, confiscate_reason, unused_trip, unused_bonus_trip, confiscate_amount, pass_average_trip_fare, operating_day, equipment_number, operator_id, transaction_mac_version, transaction_mac_value, bts_extended_data_format_version_number, bts_extension_data_ar_sequence_number, bts_extension_data_device_message_sequence_number, bts_extension_data_station_id, bts_extension_data_equipment_id, bts_extension_data_equipment_number, bts_extension_data_array_number, bts_extension_data_cloud_id_ar_sequence_number, bts_extension_data_cloud_id_device_message_sequence_number, bts_ext_spare'

UNION ALL 

SELECT coalesce(message_type::text,'0') 
|| ', ' ||    coalesce(payload_version_number::text,'0') 
|| ', ' ||    coalesce(message_sequence_number::text,'0') 
|| ', ' ||    coalesce(bss_equipment_id::text,'0') 
|| ', ' ||    coalesce(length_of_message::text,'0') 
|| ', ' ||    coalesce(transaction_id::text,'0') 
|| ', ' ||    coalesce((transaction_date_and_time + interval '7 hours' )::text,'0') 
|| ', ' ||    coalesce(service_provider_id::text,'0') 
|| ', ' ||    coalesce(unconfirmed_indicator::text,'0') 
|| ', ' ||    coalesce(project_id::text,'0') 
|| ', ' ||    coalesce(issuer_id::text,'0') 
|| ', ' ||    coalesce(card_id::text,'0') 
|| ', ' ||    coalesce(card_type::text,'0') 
|| ', ' ||    coalesce(lifecycle_counter::text,'0') 
|| ', ' ||    coalesce(card_status::text,'0') 
|| ', ' ||    coalesce(block_flag::text,'0') 
|| ', ' ||    coalesce(test_flag::text,'0') 
|| ', ' ||    coalesce(card_sequence_number::text,'0') 
|| ', ' ||    coalesce(card_unblock_sequence_number::text,'0') 
|| ', ' ||    coalesce(personalization_flag::text,'0') 
|| ', ' ||    coalesce(pass_identification_number::text,'0') 
|| ', ' ||    coalesce(sp_id_of_pass_issuer::text,'0') 
|| ', ' ||    coalesce(pass_status::text,'0') 
|| ', ' ||    coalesce(product_type_id::text,'0') 
|| ', ' ||    coalesce(pass_valid_start_date::text,'0') 
|| ', ' ||    coalesce(pass_valid_end_date::text,'0') 
|| ', ' ||    coalesce(pass_daily_trip_counter::text,'0') 
|| ', ' ||    coalesce(pass_remaining_trip_counter::text,'0') 
|| ', ' ||    coalesce(confiscate_reason::text,'0') 
|| ', ' ||    coalesce(unused_trip::text,'0') 
|| ', ' ||    coalesce(unused_bonus_trip::text,'0') 
|| ', ' ||    coalesce(confiscate_amount::text,'0') 
|| ', ' ||    coalesce(pass_average_trip_fare::text,'0') 
|| ', ' ||    coalesce(operating_day::text,'0') 
|| ', ' ||    coalesce(equipment_number::text,'0') 
|| ', ' ||    coalesce(operator_id::text,'0') 
|| ', ' ||    coalesce(transaction_mac_version::text,'0') 
|| ', ' ||    coalesce(transaction_mac_value::text,'0') 
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
from public.td_stp_confiscate  
where ssid <=  (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_stp_confiscate' and ( reported = 'P' )) 
and ssid > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report  = 'td_stp_confiscate' and ( reported  = 'Y' ));
$body$
  VOLATILE
  COST 100
  ROWS 1000;

COMMENT ON FUNCTION public.td_stp_confiscate_export() IS 'Version: 201905311339
	201905311339 : new td
	';


COMMIT;
