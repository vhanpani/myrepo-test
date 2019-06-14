CREATE OR REPLACE FUNCTION public.td_stp_confiscate_exported()
  RETURNS void
  LANGUAGE sql
AS
$body$
SELECT public.td_flag_exported('td_stp_confiscate') ;
$body$
  VOLATILE
  COST 100;

COMMIT;
