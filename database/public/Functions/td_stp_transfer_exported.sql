CREATE OR REPLACE FUNCTION public.td_stp_transfer_exported()
  RETURNS void
  LANGUAGE sql
AS
$body$
SELECT public.td_flag_exported('td_stp_transfer') ;
$body$
  VOLATILE
  COST 100;

COMMIT;
