CREATE OR REPLACE FUNCTION public.td_stp_transfer_export()
  RETURNS SETOF text
  LANGUAGE sql
AS
$body$
-- update to processing only 4 row per request

SELECT public.td_flag_export('td_stp_transfer') ; 

SELECT 'message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, transaction_date_and_time, service_provider_id, unconfirmed_indicator, new_card_project_id, new_card_issuer_id, new_card_card_id, new_card_card_type, new_card_lifecycle_counter, new_card_card_status, new_card_block_flag, new_card_test_flag, new_card_card_sequence_number, new_card_card_unblock_sequence_number, new_card_personalisation_flag, old_card_project_id, old_card_issuer_id, old_card_card_id, old_card_card_type, old_card_lifecycle_counter, old_card_card_status, old_card_block_flag, old_card_test_flag, old_card_card_sequence_number, old_card_card_unblock_sequence_number, old_card_personalisation_flag, pass_1_pass_identification_number, pass_1_sp_id_of_pass_issuer, pass_1_pass_status, pass_1_pass_type_id, pass_1_pass_valid_start_date, pass_1_pass_valid_end_date, pass_1_pass_daily_trip_counter, pass_1_pass_remaining_trip_counter, pass_2_pass_identification_number, pass_2_sp_id_of_pass_issuer, pass_2_pass_status, pass_2_pass_type_id, pass_2_pass_valid_start_date, pass_2_pass_valid_end_date, pass_2_pass_daily_trip_counter, pass_2_pass_remaining_trip_counter, stp_1_pass_price, stp_1_pass_validity_duration_mode, stp_1_pass_first_use_expiry_date, stp_1_pass_fixed_start_date, stp_1_pass_fixed_end_date, stp_1_pass_validity_duration, stp_1_number_of_trips_issued, stp_1_pass_average_trip_fare, stp_1_pass_daily_trip_limit, stp_1_pass_bonus_time_added, stp_1_pass_slot_no, stp_2_pass_price, stp_2_pass_validity_duration_mode, stp_2_pass_first_use_expiry_date, stp_2_pass_fixed_start_date, stp_2_pass_fixed_end_date, stp_2_pass_validity_duration, stp_2_number_of_trips_issued, stp_2_pass_average_trip_fare, stp_2_pass_daily_trip_limit, stp_2_pass_bonus_time_added, stp_2_pass_slot_no, operating_day, equipment_number, operator_id, transaction_mac_version, transaction_mac_value, bts_extended_data_format_version_number, bts_extension_data_ar_sequence_number, bts_extension_data_device_message_sequence_number, bts_extension_data_station_id, bts_extension_data_equipment_id, bts_extension_data_equipment_number, bts_extension_data_array_number, bts_extension_data_cloud_id_ar_sequence_number, bts_extension_data_cloud_id_device_message_sequence_number, bts_ext_spare'

UNION ALL 

select coalesce(message_type::text,'0') 
|| ', ' ||    coalesce(payload_version_number::text,'0') 
|| ', ' ||    coalesce(message_sequence_number::text,'0') 
|| ', ' ||    coalesce(bss_equipment_id::text,'0') 
|| ', ' ||    coalesce(length_of_message::text,'0') 
|| ', ' ||    coalesce(transaction_id::text,'0') 
|| ', ' ||    coalesce((transaction_date_and_time + interval '7 hours' )::text,'0') 
|| ', ' ||    coalesce(service_provider_id::text,'0') 
|| ', ' ||    coalesce(unconfirmed_indicator::text,'0') 
|| ', ' ||    coalesce(new_card_project_id::text,'0') 
|| ', ' ||    coalesce(new_card_issuer_id::text,'0') 
|| ', ' ||    coalesce(new_card_card_id::text,'0') 
|| ', ' ||    coalesce(new_card_card_type::text,'0') 
|| ', ' ||    coalesce(new_card_lifecycle_counter::text,'0') 
|| ', ' ||    coalesce(new_card_card_status::text,'0') 
|| ', ' ||    coalesce(new_card_block_flag::text,'0') 
|| ', ' ||    coalesce(new_card_test_flag::text,'0') 
|| ', ' ||    coalesce(new_card_card_sequence_number::text,'0') 
|| ', ' ||    coalesce(new_card_card_unblock_sequence_number::text,'0') 
|| ', ' ||    coalesce(new_card_personalisation_flag::text,'0') 
|| ', ' ||    coalesce(old_card_project_id::text,'0') 
|| ', ' ||    coalesce(old_card_issuer_id::text,'0') 
|| ', ' ||    coalesce(old_card_card_id::text,'0') 
|| ', ' ||    coalesce(old_card_card_type::text,'0') 
|| ', ' ||    coalesce(old_card_lifecycle_counter::text,'0') 
|| ', ' ||    coalesce(old_card_card_status::text,'0') 
|| ', ' ||    coalesce(old_card_block_flag::text,'0') 
|| ', ' ||    coalesce(old_card_test_flag::text,'0') 
|| ', ' ||    coalesce(old_card_card_sequence_number::text,'0') 
|| ', ' ||    coalesce(old_card_card_unblock_sequence_number::text,'0') 
|| ', ' ||    coalesce(old_card_personalisation_flag::text,'0') 
|| ', ' ||    coalesce(pass_1_pass_identification_number::text,'0') 
|| ', ' ||    coalesce(pass_1_sp_id_of_pass_issuer::text,'0') 
|| ', ' ||    coalesce(pass_1_pass_status::text,'0') 
|| ', ' ||    coalesce(pass_1_pass_type_id::text,'0') 
|| ', ' ||    coalesce(pass_1_pass_valid_start_date::text,'0') 
|| ', ' ||    coalesce(pass_1_pass_valid_end_date::text,'0') 
|| ', ' ||    coalesce(pass_1_pass_daily_trip_counter::text,'0') 
|| ', ' ||    coalesce(pass_1_pass_remaining_trip_counter::text,'0') 
|| ', ' ||    coalesce(pass_2_pass_identification_number::text,'0') 
|| ', ' ||    coalesce(pass_2_sp_id_of_pass_issuer::text,'0') 
|| ', ' ||    coalesce(pass_2_pass_status::text,'0') 
|| ', ' ||    coalesce(pass_2_pass_type_id::text,'0') 
|| ', ' ||    coalesce(pass_2_pass_valid_start_date::text,'0') 
|| ', ' ||    coalesce(pass_2_pass_valid_end_date::text,'0') 
|| ', ' ||    coalesce(pass_2_pass_daily_trip_counter::text,'0') 
|| ', ' ||    coalesce(pass_2_pass_remaining_trip_counter::text,'0') 
|| ', ' ||    coalesce(stp_1_pass_price::text,'0') 
|| ', ' ||
   CASE   
      WHEN stp_1_pass_validity_duration_mode = 'from_issue' THEN '1'  
      WHEN stp_1_pass_validity_duration_mode = 'from_first_use' THEN '2'
      WHEN stp_1_pass_validity_duration_mode = 'fixed_date' THEN '3'
      ELSE '0' 
   END  
|| ', ' ||    coalesce(stp_1_pass_first_use_expiry_date::text,'0') 
|| ', ' ||    coalesce(stp_1_pass_fixed_start_date::text,'0') 
|| ', ' ||    coalesce(stp_1_pass_fixed_end_date::text,'0') 
|| ', ' ||    coalesce(stp_1_pass_validity_duration::text,'0') 
|| ', ' ||    coalesce(stp_1_number_of_trips_issued::text,'0') 
|| ', ' ||    coalesce(stp_1_pass_average_trip_fare::text,'0') 
|| ', ' ||    coalesce(stp_1_pass_daily_trip_limit::text,'0') 
|| ', ' ||    coalesce(stp_1_pass_bonus_time_added::text,'0') 
|| ', ' ||    coalesce(stp_1_pass_slot_no::text,'0') 
|| ', ' ||    coalesce(stp_2_pass_price::text,'0') 
|| ', ' ||
   CASE   
      WHEN stp_2_pass_validity_duration_mode = 'from_issue' THEN '1'  
      WHEN stp_2_pass_validity_duration_mode = 'from_first_use' THEN '2'
      WHEN stp_2_pass_validity_duration_mode = 'fixed_date' THEN '3'
      ELSE '0' 
   END  
|| ', ' ||    coalesce(stp_2_pass_first_use_expiry_date::text,'0') 
|| ', ' ||    coalesce(stp_2_pass_fixed_start_date::text,'0') 
|| ', ' ||    coalesce(stp_2_pass_fixed_end_date::text,'0') 
|| ', ' ||    coalesce(stp_2_pass_validity_duration::text,'0') 
|| ', ' ||    coalesce(stp_2_number_of_trips_issued::text,'0') 
|| ', ' ||    coalesce(stp_2_pass_average_trip_fare::text,'0') 
|| ', ' ||    coalesce(stp_2_pass_daily_trip_limit::text,'0') 
|| ', ' ||    coalesce(stp_2_pass_bonus_time_added::text,'0') 
|| ', ' ||    coalesce(stp_2_pass_slot_no::text,'0') 
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
from public.td_stp_transfer
where ssid <=  (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_stp_transfer' and ( reported = 'P' )) 
and ssid > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report  = 'td_stp_transfer' and ( reported  = 'Y' ));
$body$
  VOLATILE
  COST 100
  ROWS 1000;

COMMENT ON FUNCTION public.td_stp_transfer_export() IS 'Version: 201905311339
	201905311339 : new td
	';


COMMIT;
