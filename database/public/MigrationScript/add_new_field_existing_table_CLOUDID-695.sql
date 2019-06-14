ALTER TABLE public.td_stp_issue ADD COLUMN IF NOT EXISTS payment_means_4_amount_received integer;
ALTER TABLE public.td_stp_issue ADD COLUMN IF NOT EXISTS payment_means_4_identification_number integer;
ALTER TABLE public.td_stp_issue ADD COLUMN IF NOT EXISTS operator_id text;

ALTER TABLE public.td_svc_upgrade ADD COLUMN IF NOT EXISTS operator_id text;

COMMIT;
