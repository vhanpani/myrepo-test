ALTER TABLE td_stp_entry
 ADD COLUMN process_time timestamp without time zone,
 ADD COLUMN offline_replace_recovery "char";

ALTER TABLE td_stp_exit
 ADD COLUMN process_time timestamp without time zone,
 ADD COLUMN offline_replace_recovery "char";

ALTER TABLE td_svc_entry
 ADD COLUMN process_time timestamp without time zone,
 ADD COLUMN offline_replace_recovery "char";

ALTER TABLE td_svc_exit
 ADD COLUMN process_time timestamp without time zone,
 ADD COLUMN offline_replace_recovery "char";
 
 
