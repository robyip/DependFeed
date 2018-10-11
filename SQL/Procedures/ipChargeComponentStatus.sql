USE DataWarehouse;

DROP PROCEDURE IF EXISTS ipChargeComponentStatus;

DELIMITER //

CREATE PROCEDURE ipChargeComponentStatus ()
SQL SECURITY INVOKER
MODIFIES SQL DATA
DETERMINISTIC
COMMENT 'Process the active ChargeComponent feed file'
/*******************************************************************************
*
* Project:	PlusNet Data Warehouse
* Object:	Procedure
* Name:		ipChargeComponentStatus
*
* Input Parameters
*
* Output Parameters
*
* Modification History
* 10/01/2011	Created JBR
* 2016-08-08    migrated alangridge
*
*******************************************************************************/

BEGIN
	/* Variables */
	DECLARE vInfoText	VARCHAR(32) DEFAULT 'ipChargeComponentStatus: ';
	DECLARE vParams		VARCHAR(100);
	DECLARE vError		BOOLEAN DEFAULT FALSE;
	DECLARE	vRawDataSets	INT DEFAULT 0;

	/* Handlers */

	/* Initialise */
	SET vParams = ' ';
/*	SET vParams = CONCAT('Input Parameters: ', 'p1 = ', p1...) */
	CALL ipLogInfo (CONCAT(vInfoText, 'Started ', vParams));

	/* Procedure Body */
	/* Check there are no duplicate data sets in the Raw table */
	SELECT
		COUNT(DISTINCT DATE(InsertDate))
	FROM
		tblChargeComponentStatusRaw INTO vRawDataSets;

	IF @@error_count > 0 OR vRawDataSets <> 1 THEN
		SET vError = TRUE;
		CALL ipLogError (CONCAT(vInfoText, 'Error in tblChargeComponentStatusRaw. vRawDataSets = ', IFNULL(vRawDataSets, 0)));
	ELSE
		BEGIN /* Quarantine Bad Records */
			/* Variables */
			DECLARE vInfoText	VARCHAR(34) DEFAULT 'ChargeComponentStatusQuarantine: ';
			DECLARE vAffected	INT UNSIGNED DEFAULT 0;
			DECLARE vRowCount	INT UNSIGNED DEFAULT 0;

			CALL ipLogInfo (CONCAT(vInfoText, 'Started '));

			/* INVALID NULL Check */
			INSERT INTO
				tblChargeComponentStatusQuarantine
			SELECT
				ComponentId,
				ServiceComponentId,
				InsertDate,
				'INVALID NULL'
			FROM
				tblChargeComponentStatusRaw
			WHERE
				ComponentId IS NULL OR
				ServiceComponentId IS NULL OR
				InsertDate IS NULL;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;
				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE FROM
						tblChargeComponentStatusRaw
					WHERE
						ComponentId IS NULL OR
						ServiceComponentId IS NULL OR
						InsertDate IS NULL;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL ipLogError (CONCAT(vInfoText, 'Error In NULL Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL ipLogError (CONCAT(vInfoText, 'Error In NULL Check INSERT'));
			END IF;

			/* INVALID DUPLICATES Check */
			TRUNCATE TABLE tblChargeComponentStatusDuplicate;

			IF @@error_count = 0 THEN

				INSERT tblChargeComponentStatusDuplicate
				SELECT ComponentId
				FROM tblChargeComponentStatusRaw
				GROUP BY ComponentId
				HAVING COUNT(*) > 1;

				IF @@error_count = 0 THEN

					INSERT INTO
						tblChargeComponentStatusQuarantine
					SELECT
						r.ComponentId,
						r.ServiceComponentId,
						r.InsertDate,
						'INVALID DUPLICATES'
					FROM
						tblChargeComponentStatusRaw r INNER JOIN
						tblChargeComponentStatusDuplicate d ON
							r.ComponentId = d.ComponentId;

					IF @@error_count = 0 THEN

						SELECT ROW_COUNT() INTO vRowCount;

						IF vRowCount > 0 THEN
							SET vAffected = vAffected + vRowCount;

							DELETE
								r
							FROM
								tblChargeComponentStatusRaw r INNER JOIN
								tblChargeComponentStatusQuarantine q ON
									r.ComponentId = q.ComponentId AND
									r.InsertDate = q.InsertDate;

							IF @@error_count > 0 THEN
								SET vError = TRUE;
								CALL ipLogError (CONCAT(vInfoText, 'Error In DUPLICATES Check DELETE'));
							END IF;
						END IF;
					ELSE
						SET vError = TRUE;
						CALL ipLogError (CONCAT(vInfoText, 'Error In DUPLICATES Check INSERT 2'));
					END IF;
				ELSE
					SET vError = TRUE;
					CALL ipLogError (CONCAT(vInfoText, 'Error In DUPLICATES Check INSERT 1'));
				END IF;
			ELSE
				SET vError = TRUE;
				CALL ipLogError (CONCAT(vInfoText, 'Error In DUPLICATES Check TRUNCATE'));
			END IF;

			/* INVALID RANGE Check */
			INSERT INTO
				tblChargeComponentStatusQuarantine
			SELECT
				ComponentId,
				ServiceComponentId,
				InsertDate,
				'INVALID RANGE'
			FROM
				tblChargeComponentStatusRaw
			WHERE
				ComponentId < 1 OR
				ComponentId > 4294967294 OR
				ServiceComponentId < 1 OR
				ServiceComponentId > 65534;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;

				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE FROM
						tblChargeComponentStatusRaw
					WHERE
						ComponentId < 1 OR
						ComponentId > 4294967294 OR
						ServiceComponentId < 1 OR
						ServiceComponentId > 65534;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL ipLogError (CONCAT(vInfoText, 'Error In RANGE Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL ipLogError (CONCAT(vInfoText, 'Error In RANGE Check INSERT'));
			END IF;

			/* INVALID DATE Check */
			INSERT INTO
				tblChargeComponentStatusQuarantine
			SELECT
				ComponentId,
				ServiceComponentId,
				InsertDate,
				'INVALID DATE'
			FROM
				tblChargeComponentStatusRaw
			WHERE
				STR_TO_DATE(InsertDate, '%Y-%m-%d') IS NULL;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;

				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE FROM
						tblChargeComponentStatusRaw
					WHERE
						STR_TO_DATE(InsertDate, '%Y-%m-%d') IS NULL;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL ipLogError (CONCAT(vInfoText, 'Error In DATE Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL ipLogError (CONCAT(vInfoText, 'Error In DATE Check INSERT'));
			END IF;

			/* INVALID ComponentId Check */
/* TAKE THIS OUT UNTIL I FIGURE OUT IF I CAN MOVE THE COMPONENT STATUS FEED TIMING BODGE 
			INSERT INTO
				tblChargeComponentStatusQuarantine
			SELECT
				r.ComponentId,
				r.ServiceComponentId,
				r.InsertDate,
				'INVALID ComponentId'
			FROM
				tblChargeComponentStatusRaw r LEFT JOIN
				tblComponentStatus c ON r.ComponentId = c.ComponentId
			WHERE
				c.ComponentId IS NULL;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;

				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE r
					FROM
						tblChargeComponentStatusRaw r LEFT JOIN
						tblComponentStatus c ON r.ComponentId = c.ComponentId
					WHERE
						c.ComponentId IS NULL;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In ComponentId Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In ComponentId Check INSERT'));
			END IF;
*/
			/* INVALID ChargeableComponent Check */
			INSERT INTO
				tblChargeComponentStatusQuarantine
			SELECT
				r.ComponentId,
				r.ServiceComponentId,
				r.InsertDate,
				'INVALID ChargeableComponent'
			FROM
				tblChargeComponentStatusRaw r LEFT JOIN
				tblChargeableComponent c ON r.ServiceComponentId = c.ServiceComponentId
			WHERE
				c.ServiceComponentId IS NULL;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;

				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE r
					FROM
						tblChargeComponentStatusRaw r LEFT JOIN
						tblChargeableComponent c ON r.ServiceComponentId = c.ServiceComponentId
					WHERE
						c.ServiceComponentId IS NULL;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In ChargeableComponent Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In ChargeableComponent Check INSERT'));
			END IF;

			/* Log completion */
			IF vError = TRUE THEN
				CALL ipLogError (CONCAT(vInfoText, 'Completed with Error. Rows Affected: ', vAffected));
			ELSE
				IF vAffected <> 0 THEN
					CALL ipLogWarn (CONCAT(vInfoText, 'Completed. Rows Affected: ', vAffected));
				ELSE
					CALL ipLogInfo (CONCAT(vInfoText, 'Completed.'));
				END IF;

				BEGIN /* Load verified data to the Live Table */

					/* Variables */
					DECLARE vInfoText	VARCHAR(32) DEFAULT 'LoadChargeComponentStatus: ';
					DECLARE vInsertDate	DATE;
					DECLARE vRowCount,
						vUpdated,
						vInserted	INT UNSIGNED DEFAULT 0;

					CALL ipLogInfo (CONCAT(vInfoText, 'Started ', vParams));

					/* get the insert date from the raw table */
					SELECT MAX(InsertDate)
					FROM tblChargeComponentStatusRaw INTO vInsertDate;

					/* update To date for changed or reactivated records */
					UPDATE
						tblChargeComponentStatus s INNER JOIN
						tblChargeComponentStatusRaw r ON
							s.ComponentId = r.ComponentId
					SET
						s.EffectiveTo = DATE_SUB(r.InsertDate, INTERVAL 1 DAY)
					WHERE
						s.EffectiveTo IS NULL AND
						s.ActiveFlag = 0;

					IF @@error_count = 0 THEN
						SELECT ROW_COUNT() INTO vUpdated;

						/* update To date for records becoming inactive */
						UPDATE
							tblChargeComponentStatus s LEFT JOIN
							tblChargeComponentStatusRaw r ON
								s.ComponentId = r.ComponentId
						SET
							s.EffectiveTo = DATE_SUB(vInsertDate, INTERVAL 1 DAY)
						WHERE
							s.EffectiveTo IS NULL AND
							s.ActiveFlag = 1 AND
							r.ComponentId IS NULL;

						IF @@error_count = 0 THEN
							SELECT ROW_COUNT() INTO vRowCount;
							SET vUpdated = vUpdated + vRowCount;

							/* insert new or updated active records */
							INSERT
								tblChargeComponentStatus
									(ComponentId,
									ServiceComponentId,
									ActiveFlag,
									EffectiveFrom,
									EffectiveTo)
							SELECT
								r.ComponentId,
								r.ServiceComponentId,
								1,
								r.InsertDate AS EffectiveFrom,
								NULL AS EffectiveTo
							FROM
								tblChargeComponentStatusRaw r LEFT JOIN
								tblChargeComponentStatus s ON
									r.ComponentId = s.ComponentId
							WHERE
								s.ComponentId IS NULL OR
								s.EffectiveTo = DATE_SUB(r.InsertDate, INTERVAL 1 DAY);

							IF @@error_count = 0 THEN
								SELECT ROW_COUNT() INTO vInserted;

								/* insert newly inactive records */
								INSERT
									tblChargeComponentStatus
										(ComponentId,
										ServiceComponentId,
										ActiveFlag,
										EffectiveFrom,
										EffectiveTo)
								SELECT
									s.ComponentId,
									s.ServiceComponentId,
									0,
									vInsertDate AS EffectiveFrom,
									NULL AS EffectiveTo
								FROM
									tblChargeComponentStatus s LEFT JOIN
									tblChargeComponentStatusRaw r ON
										s.ComponentId = r.ComponentId
								WHERE
									r.ComponentId IS NULL AND
									s.EffectiveTo = DATE_SUB(vInsertDate, INTERVAL 1 DAY);

								IF @@error_count = 0 THEN
									SELECT ROW_COUNT() INTO vRowCount;
									SET vInserted = vInserted + vRowCount;
								ELSE
									SET vError = TRUE;
									CALL ipLogError (CONCAT(vInfoText, 'Error In INSERT 2'));
								END IF;
							ELSE
								SET vError = TRUE;
								CALL ipLogError (CONCAT(vInfoText, 'Error In INSERT 1'));
							END IF;
						ELSE
							SET vError = TRUE;
							CALL ipLogError (CONCAT(vInfoText, 'Error In UPDATE 2'));
						END IF;
					ELSE
						SET vError = TRUE;
						CALL ipLogError (CONCAT(vInfoText, 'Error In UPDATE 1'));
					END IF;


					/* Log completion */
					IF vError = TRUE THEN
						CALL ipLogError (CONCAT(vInfoText, 'Completed with Error. Inserted: ',
											vInserted, ' Updated: ', vUpdated));
					ELSE
						/* Data imported ok so empty the Raw table */
						TRUNCATE TABLE tblChargeComponentStatusRaw;
						CALL ipLogInfo (CONCAT(vInfoText, 'Completed. Inserted: ', vInserted,
											' Updated: ', vUpdated));
					END IF;
				END;
			END IF;
		END;
	END IF;

	/* Log completion */
	SET vParams = ' ';
/*      SET vParams = CONCAT('Output Parameters: ', 'p1 = ', p1...)  */

	IF vError = TRUE THEN
		CALL ipLogError (CONCAT(vInfoText, 'Completed with Error. ', vParams));
	ELSE
		CALL ipLogInfo (CONCAT(vInfoText, 'Completed. ', vParams));
	END IF;
END; //

DELIMITER ;

