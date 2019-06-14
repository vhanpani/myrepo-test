CREATE OR REPLACE FUNCTION public.td_preparation()
  RETURNS void
  LANGUAGE plpgsql
AS
$body$
DECLARE 
newRow integer;
startTime timestamp;
BEGIN

-- td_stp_entry 4.50 m
startTime := timeofday()::timestamp;
  
insert into td_stp_entry 
(message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, pin, sp_id, pass_status, passtype, pass_valid_startdate, pass_valid_enddate, pass_valid_trip_counter, pass_remain_trip, operating_day, equipment_no, entry_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery)
SELECT message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, pin, sp_id, pass_status, passtype, pass_valid_startdate, pass_valid_enddate, pass_valid_trip_counter, pass_remain_trip, operating_day, equipment_no, entry_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery
FROM v_td_stp_entry
where SSID > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_stp_entry')  and SSID < (now() - (15 *  interval '1 minute')) ;

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_stp_entry',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_stp_entry WHERE offline_replace_recovery = '0' ) );
END IF;

-- td_stp_entry_offline 
startTime := timeofday()::timestamp;
  
insert into td_stp_entry 
(message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, pin, sp_id, pass_status, passtype, pass_valid_startdate, pass_valid_enddate, pass_valid_trip_counter, pass_remain_trip, operating_day, equipment_no, entry_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery)
SELECT message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, pin, sp_id, pass_status, passtype, pass_valid_startdate, pass_valid_enddate, pass_valid_trip_counter, pass_remain_trip, operating_day, equipment_no, entry_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery
FROM v_td_stp_entry_offline_replace_recovery
where SSID > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_stp_entry_offline_replace_recovery')  and SSID < (now() - (15 *  interval '1 minute'))  ;

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_stp_entry_offline_replace_recovery',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_stp_entry  WHERE offline_replace_recovery = '1' ) );
END IF;

-- td_stp_exit 
startTime := timeofday()::timestamp;
  
insert into td_stp_exit 
(message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, pin, sp_id, pass_status, passtype, pass_valid_startdate, pass_valid_enddate, pass_valid_trip_counter, pass_remain_trip, operating_day, equipment_no, fare, entry_location, entry_equipno, exit_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, entry_datetime, process_time,offline_replace_recovery)
SELECT message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, pin, sp_id, pass_status, passtype::integer, pass_valid_startdate, pass_valid_enddate, pass_valid_trip_counter, pass_remain_trip, operating_day, equipment_no, fare, entry_location, entry_equipno, exit_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, entry_datetime, process_time,offline_replace_recovery
FROM v_td_stp_exit
where SSID > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_stp_exit') and SSID < (now() - (15 *  interval '1 minute')) ;

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_stp_exit',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_stp_exit WHERE offline_replace_recovery = '0' ) );
END IF;

-- td_stp_exit_offline 
startTime := timeofday()::timestamp;
  
insert into td_stp_exit 
(message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, pin, sp_id, pass_status, passtype, pass_valid_startdate, pass_valid_enddate, pass_valid_trip_counter, pass_remain_trip, operating_day, equipment_no, fare, entry_location, entry_equipno, exit_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, entry_datetime, process_time,offline_replace_recovery)
SELECT message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, pin, sp_id, pass_status, passtype::integer, pass_valid_startdate, pass_valid_enddate, pass_valid_trip_counter, pass_remain_trip, operating_day, equipment_no, fare, entry_location, entry_equipno, exit_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, entry_datetime, process_time,offline_replace_recovery
FROM v_td_stp_exit_offline_replace_recovery
where SSID > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_stp_exit_offline_replace_recovery') and SSID < (now() - (15 *  interval '1 minute')) ;

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_stp_exit_offline_replace_recovery',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_stp_exit WHERE offline_replace_recovery = '1' ) );
END IF;

-- td_stp_issue
startTime := timeofday()::timestamp;
  
insert into td_stp_issue 
(message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, transaction_value, payment_mean_type, payment_amount1_val, payment_amount1_num, payment_amount2_val, payment_amount2_num, payment_amount3_val, payment_amount3_num, pin, sp_id, pass_status, passtype, pass_valid_startdate, pass_valid_enddate, pass_valid_trip_counter, pass_remain_trip, pass_price, validity_duration_mode, pass_first_use_expire_date, pass_fixed_start_date, pass_fixed_end_date, pass_validity_duration, pass_num_of_trip_issued, pass_average_trip_fare, pass_daily_trip_limit, pass_bonus, pass_slotno, operating_day, equipment_no, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, payment_means_4_amount_received, payment_means_4_identification_number, operator_id)
SELECT message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, transaction_value, payment_mean_type, payment_amount1_val, payment_amount1_num, payment_amount2_val, payment_amount2_num, payment_amount3_val, payment_amount3_num, pin, sp_id, pass_status, passtype::integer, pass_valid_startdate, pass_valid_enddate, pass_valid_trip_counter, pass_remain_trip, pass_price, validity_duration_mode, pass_first_use_expire_date, pass_fixed_start_date, pass_fixed_end_date, pass_validity_duration, pass_num_of_trip_issued, pass_average_trip_fare, pass_daily_trip_limit, pass_bonus, pass_slotno, operating_day, equipment_no, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, payment_means_4_amount_received, payment_means_4_identification_number, operator_id
FROM v_td_stp_issue 
where SSID > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_stp_issue') and SSID < (now() - (15 *  interval '1 minute'));

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_stp_issue',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_stp_issue) );
END IF;

-- td_svc_entry 
startTime := timeofday()::timestamp;
insert into td_svc_entry 
(message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, operating_day, equipment_no, entry_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery)
SELECT message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, operating_day, equipment_no, entry_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery
FROM v_td_svc_entry
where SSID > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_svc_entry') and SSID < (now() - (15 *  interval '1 minute'));

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_svc_entry',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_svc_entry WHERE offline_replace_recovery = '0' ) );
END IF;

-- td_svc_entry_offline 
startTime := timeofday()::timestamp;
insert into td_svc_entry 
(message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, operating_day, equipment_no, entry_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery)
SELECT message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, operating_day, equipment_no, entry_location, fare_mode, array_number, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery
FROM V_td_svc_entry_offline_replace_recovery
where SSID > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_svc_entry_offline_replace_recovery') and SSID < (now() - (15 *  interval '1 minute'));

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_svc_entry_offline_replace_recovery',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_svc_entry WHERE offline_replace_recovery = '1' ) );
END IF;

-- td_svc_exit 
startTime := timeofday()::timestamp;
  
insert into td_svc_exit 
(message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, transaction_value, operating_day, equipment_no, fare, entry_location, entry_equipno, exit_location, fare_mode, array_number, fare_collection, entry_datetime, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery)
SELECT message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, transaction_value, operating_day, equipment_no, fare, entry_location, entry_equipno, exit_location, fare_mode, array_number, fare_collection, entry_datetime, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery
FROM v_td_svc_exit
where SSID > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_svc_exit') and SSID < (now() - (15 *  interval '1 minute'));

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_svc_exit',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_svc_exit WHERE offline_replace_recovery = '0' ) );
END IF;

-- td_svc_exit_offline 
startTime := timeofday()::timestamp;
  
insert into td_svc_exit 
(message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, transaction_value, operating_day, equipment_no, fare, entry_location, entry_equipno, exit_location, fare_mode, array_number, fare_collection, entry_datetime, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery)
SELECT message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, transaction_value, operating_day, equipment_no, fare, entry_location, entry_equipno, exit_location, fare_mode, array_number, fare_collection, entry_datetime, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, process_time,offline_replace_recovery
FROM v_td_svc_exit_offline_replace_recovery
where SSID > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_svc_exit_offline_replace_recovery') and SSID < (now() - (15 *  interval '1 minute'));

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_svc_exit_offline_replace_recovery',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_svc_exit WHERE offline_replace_recovery = '1') );
END IF;

-- td_svc_upgrade 
startTime := timeofday()::timestamp;
  
insert into td_svc_upgrade 
(message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, transaction_value, operating_day, equipment_no, upgrade_reason, penalty_amount, penalty_charged, cash_amount, bonus_amount, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, means, operator_id)
SELECT message_type, payload_version_number, message_sequence_number, bss_equipment_id, length_of_message, transaction_id, txn_datetime, service_provider_id, unconfirmed_indicator, project_id, issuer_id, card_id, card_type, lifecycle, card_status, block_flag, test_flag, card_sequence_number, card_unblock_sequence_number, personalization_flag, purse_value, lav_service_provider_id, lav_location_code, lav_txn_datetime, lav_amount, lav_payment, lav_equipmentid, lav_csn, transaction_value, operating_day, equipment_no, upgrade_reason, penalty_amount, penalty_charged, cash_amount, bonus_amount, trans_mac_version, trans_mac_value, bts_ext_format_version_number, bts_ext_ar_seq_num, bts_ext_device_msg_seq_num, bts_ext_station_id, bts_ext_equipment_id, bts_ext_equipment_number, bts_ext_array_number, bts_ext_cloud_id_ar_seq, bts_ext_cloud_id_dev_msg_seq, bts_ext_spare, ssid, means, operator_id 
FROM v_td_svc_upgrade
where SSID > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_svc_upgrade') and SSID < (now() - (15 *  interval '1 minute'));

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime,endtime, reported, ssid )
  VALUES ('td_svc_upgrade',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_svc_upgrade) );
END IF;

-- td_stp_issue_reversal 
startTime := timeofday()::timestamp;
  
insert into td_stp_issue_reversal  
(message_type,payload_version_number,message_sequence_number,bss_equipment_id,length_of_message,transaction_id,transaction_date_and_time,service_provider_id,unconfirmed_indicator,project_id,issuer_id,card_id,card_type,lifecycle_counter,card_status,block_flag,test_flag,card_sequence_number,card_unblock_sequence_number,personalization_flag,transaction_value,pass_identification_number,sp_id_of_pass_issuer,pass_status,product_type_id,pass_valid_start_date,pass_valid_end_date,pass_daily_trip_counter,pass_remaining_trip_counter,pass_price,pass_validity_duration_mode,pass_first_use_expiry_date,pass_fixed_start_date,pass_fixed_end_date,pass_validity_duration,pass_number_of_trip_issued,pass_average_trip_fare,pass_daily_trip_limit,pass_bonus_time_added,pass_slot_no,operating_day,equipment_number,transaction_mac_version,transaction_mac_value,bts_ext_format_version_number,bts_ext_ar_seq_num,bts_ext_device_msg_seq_num,bts_ext_station_id,bts_ext_equipment_id,bts_ext_equipment_number,bts_ext_array_number,bts_ext_cloud_id_ar_seq,bts_ext_cloud_id_dev_msg_seq,bts_ext_spare,ssid,operator_id)
SELECT message_type,payload_version_number,message_sequence_number,bss_equipment_id,length_of_message,transaction_id,transaction_date_and_time,service_provider_id,unconfirmed_indicator,project_id,issuer_id,card_id,card_type,lifecycle_counter,card_status,block_flag,test_flag,card_sequence_number,card_unblock_sequence_number,personalization_flag,transaction_value::integer,pass_identification_number,sp_id_of_pass_issuer,pass_status,product_type_id::integer,pass_valid_start_date,pass_valid_end_date,pass_daily_trip_counter,pass_remaining_trip_counter,pass_price::integer,pass_validity_duration_mode,pass_first_use_expiry_date,pass_fixed_start_date,pass_fixed_end_date,pass_validity_duration,pass_number_of_trip_issued,pass_average_trip_fare,pass_daily_trip_limit,pass_bonus_time_added,pass_slot_no,operating_day,equipment_number,transaction_mac_version,transaction_mac_value,bts_ext_format_version_number,bts_ext_ar_seq_num,bts_ext_device_msg_seq_num,bts_ext_station_id,bts_ext_equipment_id,bts_ext_equipment_number,bts_ext_array_number,bts_ext_cloud_id_ar_seq,bts_ext_cloud_id_dev_msg_seq,bts_ext_spare,ssid,operator_id 
FROM v_td_stp_issue_reversal 
where ssid > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_stp_issue_reversal') and ssid < (now() - (15 *  interval '1 minute'));

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_stp_issue_reversal',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_stp_issue_reversal ) );
END IF;

-- td_stp_confiscate 
startTime := timeofday()::timestamp;
  
insert into td_stp_confiscate  
(message_type,payload_version_number,message_sequence_number,bss_equipment_id,length_of_message,transaction_id,transaction_date_and_time,service_provider_id,unconfirmed_indicator,project_id,issuer_id,card_id,card_type,lifecycle_counter,card_status,block_flag,test_flag,card_sequence_number,card_unblock_sequence_number,personalization_flag,pass_identification_number,sp_id_of_pass_issuer,pass_status,product_type_id,pass_valid_start_date,pass_valid_end_date,pass_daily_trip_counter,pass_remaining_trip_counter,confiscate_reason,unused_trip,unused_bonus_trip,confiscate_amount,pass_average_trip_fare,operating_day,equipment_number,operator_id,transaction_mac_version,transaction_mac_value,bts_ext_format_version_number,bts_ext_ar_seq_num,bts_ext_device_msg_seq_num,bts_ext_station_id,bts_ext_equipment_id,bts_ext_equipment_number,bts_ext_array_number,bts_ext_cloud_id_ar_seq,bts_ext_cloud_id_dev_msg_seq,bts_ext_spare,ssid) 
SELECT message_type,payload_version_number,message_sequence_number,bss_equipment_id,length_of_message,transaction_id,transaction_date_and_time,service_provider_id,unconfirmed_indicator,project_id,issuer_id,card_id,card_type,lifecycle_counter,card_status,block_flag,test_flag,card_sequence_number,card_unblock_sequence_number,personalization_flag,pass_identification_number,sp_id_of_pass_issuer,pass_status,product_type_id::integer,pass_valid_start_date,pass_valid_end_date,pass_daily_trip_counter,pass_remaining_trip_counter,confiscate_reason,unused_trip,unused_bonus_trip,confiscate_amount::integer,pass_average_trip_fare,operating_day,equipment_number,operator_id,transaction_mac_version,transaction_mac_value,bts_ext_format_version_number,bts_ext_ar_seq_num,bts_ext_device_msg_seq_num,bts_ext_station_id,bts_ext_equipment_id,bts_ext_equipment_number,bts_ext_array_number,bts_ext_cloud_id_ar_seq,bts_ext_cloud_id_dev_msg_seq,bts_ext_spare,ssid 
FROM v_td_stp_confiscate 
where ssid > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_stp_confiscate') and ssid < (now() - (15 *  interval '1 minute'));

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_stp_confiscate',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_stp_confiscate ) );
END IF;

-- td_stp_transfer 
startTime := timeofday()::timestamp;
  
insert into td_stp_transfer  
(message_type,payload_version_number,message_sequence_number,bss_equipment_id,length_of_message,transaction_id,transaction_date_and_time,service_provider_id,unconfirmed_indicator,new_card_project_id,new_card_issuer_id,new_card_card_id,new_card_card_type,new_card_lifecycle_counter,new_card_card_status,new_card_block_flag,new_card_test_flag,new_card_card_sequence_number,new_card_card_unblock_sequence_number,new_card_personalisation_flag,old_card_project_id,old_card_issuer_id,old_card_card_id,old_card_card_type,old_card_lifecycle_counter,old_card_card_status,old_card_block_flag,old_card_test_flag,old_card_card_sequence_number,old_card_card_unblock_sequence_number,old_card_personalisation_flag,pass_1_pass_identification_number,pass_1_sp_id_of_pass_issuer,pass_1_pass_status,pass_1_pass_type_id,pass_1_pass_valid_start_date,pass_1_pass_valid_end_date,pass_1_pass_daily_trip_counter,pass_1_pass_remaining_trip_counter,pass_2_pass_identification_number,pass_2_sp_id_of_pass_issuer,pass_2_pass_status,pass_2_pass_type_id,pass_2_pass_valid_start_date,pass_2_pass_valid_end_date,pass_2_pass_daily_trip_counter,pass_2_pass_remaining_trip_counter,stp_1_pass_price,stp_1_pass_validity_duration_mode,stp_1_pass_first_use_expiry_date,stp_1_pass_fixed_start_date,stp_1_pass_fixed_end_date,stp_1_pass_validity_duration,stp_1_number_of_trips_issued,stp_1_pass_average_trip_fare,stp_1_pass_daily_trip_limit,stp_1_pass_bonus_time_added,stp_1_pass_slot_no,stp_2_pass_price,stp_2_pass_validity_duration_mode,stp_2_pass_first_use_expiry_date,stp_2_pass_fixed_start_date,stp_2_pass_fixed_end_date,stp_2_pass_validity_duration,stp_2_number_of_trips_issued,stp_2_pass_average_trip_fare,stp_2_pass_daily_trip_limit,stp_2_pass_bonus_time_added,stp_2_pass_slot_no,operating_day,equipment_number,operator_id,transaction_mac_version,transaction_mac_value,bts_ext_format_version_number,bts_ext_ar_seq_num,bts_ext_device_msg_seq_num,bts_ext_station_id,bts_ext_equipment_id,bts_ext_equipment_number,bts_ext_array_number,bts_ext_cloud_id_ar_seq,bts_ext_cloud_id_dev_msg_seq,bts_ext_spare,ssid) 
SELECT message_type,payload_version_number,message_sequence_number,bss_equipment_id,length_of_message,transaction_id,transaction_date_and_time,service_provider_id,unconfirmed_indicator,new_card_project_id,new_card_issuer_id,new_card_card_id,new_card_card_type,new_card_lifecycle_counter,new_card_card_status,new_card_block_flag,new_card_test_flag,new_card_card_sequence_number,new_card_card_unblock_sequence_number,new_card_personalisation_flag,old_card_project_id,old_card_issuer_id,old_card_card_id,old_card_card_type,old_card_lifecycle_counter,old_card_card_status,old_card_block_flag,old_card_test_flag,old_card_card_sequence_number,old_card_card_unblock_sequence_number,old_card_personalisation_flag,pass_1_pass_identification_number,pass_1_sp_id_of_pass_issuer,pass_1_pass_status,pass_1_pass_type_id::integer,pass_1_pass_valid_start_date,pass_1_pass_valid_end_date,pass_1_pass_daily_trip_counter,pass_1_pass_remaining_trip_counter,pass_2_pass_identification_number,pass_2_sp_id_of_pass_issuer,pass_2_pass_status,pass_2_pass_type_id::integer,pass_2_pass_valid_start_date,pass_2_pass_valid_end_date,pass_2_pass_daily_trip_counter,pass_2_pass_remaining_trip_counter,stp_1_pass_price::integer,stp_1_pass_validity_duration_mode,stp_1_pass_first_use_expiry_date,stp_1_pass_fixed_start_date,stp_1_pass_fixed_end_date,stp_1_pass_validity_duration,stp_1_number_of_trips_issued,stp_1_pass_average_trip_fare,stp_1_pass_daily_trip_limit,stp_1_pass_bonus_time_added,stp_1_pass_slot_no,stp_2_pass_price::integer,stp_2_pass_validity_duration_mode,stp_2_pass_first_use_expiry_date,stp_2_pass_fixed_start_date,stp_2_pass_fixed_end_date,stp_2_pass_validity_duration,stp_2_number_of_trips_issued,stp_2_pass_average_trip_fare,stp_2_pass_daily_trip_limit,stp_2_pass_bonus_time_added,stp_2_pass_slot_no,operating_day,equipment_number,operator_id,transaction_mac_version,transaction_mac_value,bts_ext_format_version_number,bts_ext_ar_seq_num,bts_ext_device_msg_seq_num,bts_ext_station_id,bts_ext_equipment_id,bts_ext_equipment_number,bts_ext_array_number,bts_ext_cloud_id_ar_seq,bts_ext_cloud_id_dev_msg_seq,bts_ext_spare,ssid
FROM v_td_stp_transfer 
where ssid > (SELECT coalesce(max(ssid), date '1970-01-01') FROM  td_ss_info WHERE report = 'td_stp_transfer') and ssid < (now() - (15 *  interval '1 minute'));

GET DIAGNOSTICS newRow = ROW_COUNT;

IF (newRow > 0 ) 
THEN
  insert into td_ss_info (report, starttime, endtime, reported, ssid )
  VALUES ('td_stp_transfer',startTime,timeofday()::timestamp, 'N' , (SELECT max(ssid) FROM  td_stp_transfer ) );
END IF;

END;
$body$
  VOLATILE
  COST 100;

COMMENT ON FUNCTION td_preparation() IS 'Version: Version: 201905311409
Change log
201905311409: add fuction td_stp_issue_reversal,td_stp_confiscate
201905231111: CLOUDID-695: add column operator_id for table td_svc_upgrade,td_stp_issue and add column payment_means_4_amount_received,payment_means_4_identification_number for table td_stp_issue
201905211640: CLOUDID-695: change offline_replace_recovery N to 0 and Y to 1
201903191644: COC-141 : Edit in part of td_stp_exit_offline by editing condition from  offline_replace_recovery = ''N''  to  offline_replace_recovery = ''Y'' 
201808231600: CLOUDID-568';


COMMIT;
