CREATE OR REPLACE FUNCTION public.td_stp_issue_reversal_exported()
  RETURNS void
  LANGUAGE sql
AS
$body$
SELECT public.td_flag_exported('td_stp_issue_reversal') ;
$body$
  VOLATILE
  COST 100;

COMMIT;
