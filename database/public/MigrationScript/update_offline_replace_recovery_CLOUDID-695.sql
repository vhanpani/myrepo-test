update public.td_stp_entry set offline_replace_recovery = '0' where offline_replace_recovery = 'N';

update public.td_stp_entry set offline_replace_recovery = '1' where offline_replace_recovery = 'Y';

update public.td_stp_exit set offline_replace_recovery = '0' where offline_replace_recovery = 'N';

update public.td_stp_exit set offline_replace_recovery = '1' where offline_replace_recovery = 'Y';

update public.td_svc_entry set offline_replace_recovery = '0' where offline_replace_recovery = 'N';

update public.td_svc_entry set offline_replace_recovery = '1' where offline_replace_recovery = 'Y';

update public.td_svc_exit set offline_replace_recovery = '0' where offline_replace_recovery = 'N';

update public.td_svc_exit set offline_replace_recovery = '1' where offline_replace_recovery = 'Y';

COMMIT;