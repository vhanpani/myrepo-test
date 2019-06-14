-- FUNCTION: public.get_svc_exit_entry_station(text, text, integer, integer)

-- DROP FUNCTION public.get_svc_exit_entry_station(text, text, integer, integer);

CREATE OR REPLACE FUNCTION public.get_svc_exit_entry_station(
	pass_used_href text,
	pass_at_entry_href text,
	entry_station_id integer,
	exit_station_id integer)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    IMMUTABLE STRICT 
AS $BODY$


DECLARE
    result  integer;
BEGIN
	/* normal trip */
	/*
	IF pass_used_href = 'N' AND pass_at_entry_href = 'N'  THEN
    	result := entry_station_id;
	ELSIF (pass_used_href <> 'N' OR pass_at_entry_href <> 'N' )  AND  exit_station_id = ANY (ARRAY[36, 37, 38, 39])  and entry_station_id <> ALL (ARRAY[31, 32, 33, 34, 35, 53, 54, 55, 56, 57, 58, 59, 60, 61, 36, 37, 38, 39])  THEN
    	result := 30;
    ELSIF(pass_used_href <> 'N' OR pass_at_entry_href <> 'N' ) AND  entry_station_id <> ALL (ARRAY[36, 37, 38, 39, 31, 32, 33, 34, 35, 53, 54, 55, 56, 57, 58, 59, 60, 61])  and exit_station_id =  ANY (ARRAY[31, 32, 33, 34, 35, 53, 54, 55, 56, 57, 58, 59, 60, 61])  THEN
    	result := 1;  
	ELSE
    	result := entry_station_id;
	END IF;
	*/
	  result := entry_station_id;

    RETURN result;
END;


$BODY$;

ALTER FUNCTION public.get_svc_exit_entry_station(text, text, integer, integer)
    OWNER TO cloudid;

COMMENT ON FUNCTION public.get_svc_exit_entry_station(text, text, integer, integer)
    IS 'Version: 201902061700
201902061700 Remove condition to check BTS Border stations
201811141800 Support New Station BMA4
201802131200 CLOUDIDTST-75
';

commit;
