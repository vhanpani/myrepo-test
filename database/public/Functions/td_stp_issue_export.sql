CREATE OR REPLACE FUNCTION public.td_stp_issue_export()
  RETURNS SETOF text
  LANGUAGE sql
AS
$body$
-- update to processing only 4 row per request

SELECT public.td_flag_export('td_stp_issue') ; 

SELECT 'message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, transaction_date_and_time, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle_counter, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalisation_flag, transaction_value, payment_means_type, payment_means_1_amount_received, payment_means_1_identification_number, payment_means_2_amount_received, payment_means_2_identification_number, payment_means_3_amount_received, payment_means_3_identification_number, payment_means_4_amount_received, payment_means_4_identification_number, pass_identification_number, sp_id_of_pass_issuer, pass_status, product_type_id, pass_valid_start_date, pass_valid_end_date, pass_daily_trip_counter, pass_remaining_trip_counter, pass_price, pass_validity_duration_mode, pass_first_use_expiry_date, pass_fixed_start_date, pass_fixed_end_date, pass_validity_duration, pass_number_of_trips_issued, pass_average_trip_fare, pass_daily_trip_limit, pass_bonus_time_added, pass_slot_no, operating_day, equipment_number, operator_id, transaction_mac_version, transaction_mac_value, bts_extended_data_format_version_number, bts_extension_data_ar_sequence_number, bts_extension_data_device_message_sequence_number, bts_extension_data_station_id, bts_extension_data_equipment_id, bts_extension_data_equipment_number, bts_extension_data_array_number, bts_extension_data_cloud_id_ar_sequence_number, bts_extension_data_cloud_id_device_message_sequence_number, bts_ext_spare'

UNION ALL 

  
select coalesce(message_type::text,'0') 
|| ', ' ||    coalesce(payload_version_number::text,'0')
|| ', ' ||    coalesce(message_sequence_number::text,'0') 
|| ', ' ||    hex_to_int(coalesce(bss_equipment_id::text,'0')) 
|| ', ' ||    coalesce(length_of_message::text,'0') 
|| ', ' ||    coalesce(transaction_id::text,'0') 
|| ', ' ||    coalesce((txn_datetime + interval '7 hours')::text,'0') 
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
|| ', ' ||    coalesce(payment_mean_type::text,'0') 
|| ', ' ||    coalesce(payment_amount1_val::text,'0') 
|| ', ' ||    coalesce(payment_amount1_num::text,'0') 
|| ', ' ||    coalesce(payment_amount2_val::text,'0') 
|| ', ' ||    coalesce(payment_amount2_num::text,'0') 
|| ', ' ||    coalesce(payment_amount3_val::text,'0') 
|| ', ' ||    coalesce(payment_amount3_num::text,'0') 
|| ', ' ||    coalesce(payment_means_4_amount_received::text,'0') 
|| ', ' ||    coalesce(payment_means_4_identification_number::text,'0') 
|| ', ' ||    coalesce(pin::text,'0') 
|| ', ' ||    coalesce(sp_id::text,'0') 
|| ', ' ||    coalesce(pass_status::text,'0') 
|| ', ' ||    coalesce(passtype::text ,'0')
|| ', ' ||    coalesce(pass_valid_startdate::text,'0') 
|| ', ' ||    coalesce(pass_valid_enddate::text,'0') 
|| ', ' ||    coalesce(pass_valid_trip_counter::text,'0') 
|| ', ' ||    coalesce(pass_remain_trip::text,'0') 
|| ', ' ||    coalesce(pass_price::text,'0') 
|| ', ' ||
   CASE   
      WHEN validity_duration_mode = 'from_issue' THEN '1'  
      WHEN validity_duration_mode = 'from_first_use' THEN '2'
      WHEN validity_duration_mode = 'fixed_date' THEN '3'
      ELSE '0' 
   END  
|| ', ' ||    coalesce(pass_first_use_expire_date::text,'0') 
|| ', ' ||    coalesce(pass_fixed_start_date::text,'0') 
|| ', ' ||    coalesce(pass_fixed_end_date::text,'0') 
|| ', ' ||    coalesce(pass_validity_duration::text,'0')
|| ', ' ||    coalesce(pass_num_of_trip_issued::text,'0') 
|| ', ' ||    coalesce(pass_average_trip_fare::text,'0') 
|| ', ' ||    coalesce(pass_daily_trip_limit::text,'0') 
|| ', ' ||    coalesce(pass_bonus::text,'0') 
|| ', ' ||    coalesce(pass_slotno::text,'0') 
|| ', ' ||    coalesce(operating_day::text,'0') 
|| ', ' ||    coalesce(equipment_no::text,'0') 
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
from public.td_stp_issue  
where ssid <=  (SELECT coalesce(max(ssid), date '1970-01-01') FROM  public.td_ss_info WHERE report = 'td_stp_issue' and ( reported = 'P' )) 
and ssid > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  public.td_ss_info WHERE report  = 'td_stp_issue' and ( reported  = 'Y' ));
$body$
  VOLATILE
  COST 100
  ROWS 1000;

COMMENT ON FUNCTION public.td_stp_issue_export() IS 'Version: 201905231111
	201905231111 : CLOUDID-695: add column payment_means_4_amount_received,payment_means_4_identification_number,operator_id
	201802231900
	';


COMMIT;
