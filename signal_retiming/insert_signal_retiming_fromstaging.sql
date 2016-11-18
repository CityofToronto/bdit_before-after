/*
author: Raphael Dumas
Takes unique values from the signal retiming staging table and inserts them into their respective tables before inserting the signal retiming periods.
Then joins all these tables together to get the respective ids, splits the dates text field into a daterange and inserts into the intervention_periods main table
*/

INSERT INTO beforeafter.intervention_type (intervention)
VALUES ('signal retiming');

INSERT INTO beforeafter.routes (route_name)
SELECT DISTINCT route_name FROM beforeafter.signal_retiming_staging;

INSERT INTO beforeafter.segments (segment_name)
SELECT DISTINCT segment_name FROM beforeafter.signal_retiming_staging;

WITH date_split AS(SELECT route_name, segment_name, string_to_array(period_text, ' to ') datearray, note
  FROM beforeafter.signal_retiming_staging) 

INSERT INTO beforeafter.intervention_periods(
            intervention_type_id, route_id, segment_id, 
            period, note)
SELECT intervention_type_id, route_id, segment_id, daterange(to_date(datearray[1], 'Month DD, YYYY'), to_date(COALESCE(datearray[2],datearray[1]), 'Month DD, YYYY'), '[]') as period , note
  FROM date_split
  INNER JOIN beforeafter.intervention_type ON intervention = 'signal retiming'
  INNER JOIN beforeafter.routes USING (route_name)
  INNER JOIN beforeafter.segments USING (segment_name)