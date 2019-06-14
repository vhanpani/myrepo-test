CREATE OR REPLACE FUNCTION public.get_td_generate(td_type text, entry_station_id integer, exit_station_id integer)
  RETURNS integer
  LANGUAGE plpgsql
AS
$body$
DECLARE
		entry_type varchar;
		exit_type varchar;
    result  integer;
BEGIN
	/*Check Type of Station BTS, Border and BMA1-4*/

	entry_type := (
--  	CASE WHEN entry_station_id = ANY (ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]) THEN 'BTS' /*EXCLUDE S8, E9, INCLUDE S7*/
--  			 WHEN entry_station_id = ANY (ARRAY[29, 30]) THEN 'BMA1' /*S7-S8*/
  	CASE WHEN entry_station_id = ANY (ARRAY[2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]) THEN 'BTS' /*EXCLUDE E9*/
  			 WHEN entry_station_id = ANY (ARRAY[29]) THEN 'BMA1' /*S7*/
         WHEN entry_station_id = ANY (ARRAY[31, 32, 33, 34, 35]) THEN 'BMA2' /*E10-E14*/
  			 WHEN entry_station_id = ANY (ARRAY[36, 37, 38, 39]) THEN 'BMA3' /*S9-S12*/
  			 WHEN entry_station_id = ANY (ARRAY[53, 54, 55, 56, 57, 58, 59, 60, 61]) THEN 'BMA4' /*E15-E23 :BMA4*/
  			 WHEN entry_station_id = ANY (ARRAY[1]) THEN 'E9' /*BORDER E9*/
			   WHEN entry_station_id = ANY (ARRAY[30]) THEN 'S8' /*BORDER S8*/
    ELSE '-' END);

	exit_type := (
--  	CASE WHEN exit_station_id = ANY (ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]) THEN 'BTS' /*EXCLUDE S8, E9, INCLUDE S7*/
--  			 WHEN exit_station_id = ANY (ARRAY[29, 30]) THEN 'BMA1' /*S7-S8*/
  	CASE WHEN exit_station_id = ANY (ARRAY[2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]) THEN 'BTS' /*EXCLUDE E9*/
  			 WHEN exit_station_id = ANY (ARRAY[29]) THEN 'BMA1' /*S7*/
  			 WHEN exit_station_id = ANY (ARRAY[31, 32, 33, 34, 35]) THEN 'BMA2' /*E10-E14*/
         WHEN exit_station_id = ANY (ARRAY[36, 37, 38, 39]) THEN 'BMA3' /*S9-S12*/
  			 WHEN exit_station_id = ANY (ARRAY[53, 54, 55, 56, 57, 58, 59, 60, 61]) THEN 'BMA4' /*E15-E23 :BMA4*/
  			 WHEN exit_station_id = ANY (ARRAY[1]) THEN 'E9' /*BORDER E9*/
			   WHEN exit_station_id = ANY (ARRAY[30]) THEN 'S8' /*BORDER S8*/
  	ELSE '-' END);	
	
	/* get td generator */
	result := (
	CASE 
	  WHEN entry_type = '-' THEN 0 -- NO ENTRY STATION
	  WHEN exit_type = '-' THEN --ENTRY STATION ONLY
	    CASE 
	      WHEN td_type = 'STP ENTRY' THEN
  	      CASE WHEN entry_type IN ('BTS','BMA1','S8','E9') THEN 1 ELSE 0 END
        WHEN td_type = 'SVC ENTRY' THEN
  	      CASE WHEN entry_type IN ('BTS','BMA1','S8','E9') THEN 0 ELSE 1 END
      ELSE 0 END
	ELSE
  	CASE 
  	   /* Entry BTS */
  	   WHEN (entry_type, exit_type) IN ( ('BTS', 'BTS') ) THEN 
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('BTS', 'BMA1') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('BTS', 'BMA2') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('BTS', 'BMA3') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('BTS', 'BMA4') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('BTS', 'BMA5') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END				
  	   WHEN (entry_type, exit_type) IN ( ('BTS', 'E9') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END				
  	   WHEN (entry_type, exit_type) IN ( ('BTS', 'S8') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END				

  	   /* Entry BMA1 */
  	   WHEN (entry_type, exit_type) IN ( ('BMA1', 'BTS') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('BMA1', 'BMA1') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('BMA1', 'BMA2') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA1', 'BMA3') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA1', 'BMA4') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA1', 'BMA5') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA1', 'E9') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END				
  	   WHEN (entry_type, exit_type) IN ( ('BMA1', 'S8') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END				

  	   /* Entry BMA2 */
  	   WHEN (entry_type, exit_type) IN ( ('BMA2', 'BTS') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA2', 'BMA1') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA2', 'BMA2') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA2', 'BMA3') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA2', 'BMA4') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA2', 'BMA5') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA2', 'E9') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA2', 'S8') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	

  	   /* Entry BMA3 */
  	   WHEN (entry_type, exit_type) IN ( ('BMA3', 'BTS') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA3', 'BMA1') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA3', 'BMA2') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA3', 'BMA3') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA3', 'BMA4') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA3', 'BMA5') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA3', 'E9') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA3', 'S8') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  		   
  	   /* Entry BMA4 */
  	   WHEN (entry_type, exit_type) IN ( ('BMA4', 'BTS') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA4', 'BMA1') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA4', 'BMA2') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA4', 'BMA3') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA4', 'BMA4') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA4', 'BMA5') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA4', 'E9') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA4', 'S8') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	

  	   /* Entry BMA5 */
  	   WHEN (entry_type, exit_type) IN ( ('BMA5', 'BTS') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA5', 'BMA1') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA5', 'BMA2') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA5', 'BMA3') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA5', 'BMA4') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA5', 'BMA5') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA5', 'E9') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	
  	   WHEN (entry_type, exit_type) IN ( ('BMA5', 'S8') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 0
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 1
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END	

  	   /* Entry E9 */
  	   WHEN (entry_type, exit_type) IN ( ('E9', 'BTS') ) THEN 
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('E9', 'BMA1') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('E9', 'BMA2') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('E9', 'BMA3') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('E9', 'BMA4') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('E9', 'BMA5') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END				
  	   WHEN (entry_type, exit_type) IN ( ('E9', 'E9') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END				
  	   WHEN (entry_type, exit_type) IN ( ('E9', 'S8') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END		

  	   /* Entry S8 */
  	   WHEN (entry_type, exit_type) IN ( ('S8', 'BTS') ) THEN 
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('S8', 'BMA1') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('S8', 'BMA2') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('S8', 'BMA3') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 0
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('S8', 'BMA4') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END			
  	   WHEN (entry_type, exit_type) IN ( ('S8', 'BMA5') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 1
  		   ELSE 0 END				
  	   WHEN (entry_type, exit_type) IN ( ('S8', 'E9') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END				
  	   WHEN (entry_type, exit_type) IN ( ('S8', 'S8') ) THEN
  		   CASE WHEN td_type = 'STP ENTRY' THEN 1
  				WHEN td_type = 'STP EXIT' THEN 1
  				WHEN td_type = 'SVC ENTRY' THEN 0
  				WHEN td_type = 'SVC EXIT' THEN 0
  		   ELSE 0 END		

  	ELSE 0 END
  END);
	
  RETURN result;

END;
$body$
  IMMUTABLE
  STRICT
  COST 100;

COMMENT ON FUNCTION public.get_td_generate(text, integer, integer) IS 'Version: 201902041630
201902041630 Create function to verify trips transaction should be show in TD by TD type condition.
';


COMMIT;
