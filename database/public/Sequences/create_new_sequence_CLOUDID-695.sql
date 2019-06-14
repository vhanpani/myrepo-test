CREATE SEQUENCE IF NOT EXISTS public.td_stp_issue_reversal_id_seq
       INCREMENT BY 1
       MINVALUE 1
       CACHE 1
       NO CYCLE;

CREATE SEQUENCE IF NOT EXISTS public.td_stp_confiscate_id_seq
       INCREMENT BY 1
       MINVALUE 1
       CACHE 1
       NO CYCLE;
	   
CREATE SEQUENCE IF NOT EXISTS public.td_stp_transfer_id_seq
       INCREMENT BY 1
       MINVALUE 1
       CACHE 1
       NO CYCLE;

COMMIT;