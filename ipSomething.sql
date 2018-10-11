USE DataWarehouse Nothing else
	
DROP PROCEDURE IF EXISTS ipSomething

CREATE PROCEDURE ipSomething ()

BEGIN

/*
	Error handler and Warning handler
*/

/*

	Quarantine

*/
	-- Quarantine with schema
	INSERT INTO Staging.tblSomethingQuarantine 
	(
		col1,
		col2,
		col3,
		col4
	)
	select 
		  col1,
		col2,
		Reason,
		InsertDate
    from 
	    Staging.tblSomethingRaw AS r
		INNER JOIN  DataWarehouse.tblSomething AS l
			ON l.col1 = r.col1
	where
		r.col1 is null

	INSERT INTO tblSomethingQuarantine 
	(
		col1,
		col2,
		col3,
		col4
	)
	select 
		col1,
		col2,
		Reason,
		InsertDate
	from
		tblSomethingRaw AS r
		INNER JOIN  tblSomething AS l
			ON l.col1 = r.col1
	where
		r.col1 is null		

	-- Quanantien without Scheame
``
/*
	Load
*/


	

END

DELIMITER ;