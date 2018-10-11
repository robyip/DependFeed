USE DataWarehouse;

DROP PROCEDURE IF EXISTS ipComponentStatus;

DELIMITER //

CREATE PROCEDURE ipComponentStatus ()
SQL SECURITY INVOKER
DETERMINISTIC
COMMENT 'Process the Component feed file'
/*******************************************************************************
*
*	Project:	PlusNet Data Warehouse
*	Object:		Procedure
*	Name:		ipComponentStatus
*
*	Input Parameters
*
*	Modification History
*	13/08/2007	Created JBR
*	13/08/2007	ComponentName replaced by ServiceComponentId JBR
*	20/03/2012	ServiceId check changed to tblAccountStatusNew JBR
* 	03/01/2013	Test Account delete - Jamuna
* 	28/07/2016	Migrated - alangridge
*   07/09/2017  Workaround for BtSportApp ServiceComponentId's - iroberts
*               (Added lines 330 and 346)
*
*******************************************************************************/

BEGIN
	/* Variables */
	DECLARE vInfoText	VARCHAR(32) DEFAULT 'ipComponentStatus: ';
	DECLARE vParams		VARCHAR(100);
	DECLARE vError		BOOLEAN DEFAULT FALSE;
	DECLARE	vRawDataSets	INT DEFAULT 0;

	/* Handlers */

	/* Initialise */
	SET vParams = ' ';
/*	SET vParams = CONCAT('Input Parameters: ', 'p1 = ', p1...) */
	CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Started ', vParams));

	/* Procedure Body */

	SELECT
		COUNT(DISTINCT InsertDate) INTO vRawDataSets
	FROM
		tblComponentStatusRaw;

	IF @@error_count > 0 OR vRawDataSets <> 1 THEN
		SET vError = TRUE;
		CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error in tblComponentStatusRaw. vRawDataSets = ', IFNULL(vRawDataSets, 0)));
	ELSE
		BEGIN
			/* Variables */
			DECLARE vInfoText	VARCHAR(32) DEFAULT 'ComponentStatusQuarantine: ';
			DECLARE vAffected	INT UNSIGNED DEFAULT 0;
			DECLARE vRowCount	INT UNSIGNED DEFAULT 0;

			CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Started'));

			/* Test Account Delete */
			DELETE r 
			FROM
				tblComponentStatusRaw r INNER JOIN
				tblTestAndUpgradeAccount t USING (ServiceId);

			IF @@error_count > 0 THEN
				SET vError = TRUE;
				CALL ipLogError (CONCAT(vInfoText, 'Error In Test Account DELETE'));
			END IF;

			/* INVALID NULL Check */
			INSERT INTO
				tblComponentStatusQuarantine
			SELECT
				ComponentId,
				ServiceId,
				ServiceComponentId,
				ComponentStatus,
				CreationDate,
				InsertDate,
				'INVALID NULL'
			FROM
				tblComponentStatusRaw
			WHERE
				ComponentId IS NULL OR
				ServiceId IS NULL OR
				ServiceComponentId IS NULL OR
				ComponentStatus IS NULL OR
				CreationDate IS NULL OR
				InsertDate IS NULL;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;
				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE FROM
						tblComponentStatusRaw
					WHERE
						ComponentId IS NULL OR
						ServiceId IS NULL OR
						ServiceComponentId IS NULL OR
						ComponentStatus IS NULL OR
						CreationDate IS NULL OR
						InsertDate IS NULL;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In NULL Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In NULL Check INSERT'));
			END IF;

			/* INVALID DUPLICATES Check */
			TRUNCATE TABLE tblComponentStatusDuplicates;

			IF @@error_count = 0 THEN
				INSERT INTO
					tblComponentStatusDuplicates
				SELECT
					ComponentId
				FROM
					tblComponentStatusRaw
				GROUP BY
					ComponentId
				HAVING
					COUNT(*) > 1;

				IF @@error_count = 0 THEN
					INSERT INTO
						tblComponentStatusQuarantine
					SELECT
						r.ComponentId,
						r.ServiceId,
						r.ServiceComponentId,
						r.ComponentStatus,
						r.CreationDate,
						r.InsertDate,
						'INVALID DUPLICATES'
					FROM
						tblComponentStatusRaw r INNER JOIN
						tblComponentStatusDuplicates d ON
							r.ComponentId = d.ComponentId;

					IF @@error_count = 0 THEN
						SELECT ROW_COUNT() INTO vRowCount;

						IF vRowCount > 0 THEN
							SET vAffected = vAffected + vRowCount;

							DELETE
								r
							FROM
								tblComponentStatusRaw r INNER JOIN
								tblComponentStatusQuarantine q ON
									r.ComponentId = q.ComponentId AND
									r.InsertDate = q.InsertDate;

							IF @@error_count > 0 THEN
								SET vError = TRUE;
								CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In DUPLICATES Check DELETE'));
							END IF;
						END IF;
					ELSE
						SET vError = TRUE;
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In DUPLICATES Check INSERT 2'));
					END IF;
				ELSE
					SET vError = TRUE;
					CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In DUPLICATES Check INSERT 1'));
				END IF;
			ELSE
				SET vError = TRUE;
				CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In DUPLICATES Check TRUNCATE'));
			END IF;

			/* INVALID RANGE Check */
			INSERT INTO
				tblComponentStatusQuarantine
			SELECT
				ComponentId,
				ServiceId,
				ServiceComponentId,
				ComponentStatus,
				CreationDate,
				InsertDate,
				'INVALID RANGE'
			FROM
				tblComponentStatusRaw
			WHERE
				ComponentId < 1 OR
				ComponentId > 4294967294;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;

				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE FROM
						tblComponentStatusRaw
					WHERE
						ComponentId < 1 OR
						ComponentId > 4294967294;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In RANGE Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In RANGE Check INSERT'));
			END IF;

			/* INVALID DATE Check */
			INSERT INTO
				tblComponentStatusQuarantine
			SELECT
				ComponentId,
				ServiceId,
				ServiceComponentId,
				ComponentStatus,
				CreationDate,
				InsertDate,
				'INVALID DATE'
			FROM
				tblComponentStatusRaw
			WHERE
				STR_TO_DATE(CreationDate, '%Y-%m-%d') IS NULL OR
				STR_TO_DATE(InsertDate, '%Y-%m-%d') IS NULL;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;

				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE FROM
						tblComponentStatusRaw
					WHERE
						STR_TO_DATE(CreationDate, '%Y-%m-%d') IS NULL OR
						STR_TO_DATE(InsertDate, '%Y-%m-%d') IS NULL;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In DATE Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In DATE Check INSERT'));
			END IF;

			/* INVALID ServiceId Check */
			INSERT INTO
				tblComponentStatusQuarantine
			SELECT
				r.ComponentId,
				r.ServiceId,
				r.ServiceComponentId,
				r.ComponentStatus,
				r.CreationDate,
				r.InsertDate,
				'INVALID ServiceId'
			FROM
				tblComponentStatusRaw r LEFT JOIN
				tblAccountStatusNew s ON
					r.ServiceId = s.ServiceId AND
					s.EffectiveTo IS NULL LEFT JOIN
				tblHostmasterAccountStatus h ON
					r.ServiceId = h.ServiceId AND
					h.EffectiveTo IS NULL
			WHERE
				s.ServiceId IS NULL AND
				h.ServiceId IS NULL;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;

				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE r
					FROM
						tblComponentStatusRaw r LEFT JOIN
						tblAccountStatusNew s ON
							r.ServiceId = s.ServiceId AND
							s.EffectiveTo IS NULL LEFT JOIN
						tblHostmasterAccountStatus h ON
							r.ServiceId = h.ServiceId AND
							s.EffectiveTo IS NULL
					WHERE
						s.ServiceId IS NULL AND
						h.ServiceId IS NULL;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In ServiceId Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In ServiceId Check INSERT'));
			END IF;

			/* INVALID ServiceComponentId Check */
			INSERT INTO
				tblComponentStatusQuarantine
			SELECT
				r.ComponentId,
				r.ServiceId,
				r.ServiceComponentId,
				r.ComponentStatus,
				r.CreationDate,
				r.InsertDate,
				'INVALID ServiceComponentId'
			FROM
				tblComponentStatusRaw r LEFT JOIN
				tblServiceComponentDataSheet ds ON
					r.ServiceComponentId = ds.ServiceComponentId AND
					ds.EffectiveTo IS NULL
			WHERE
                r.ServiceComponentId NOT IN (2244,2245) AND -- BtSportApp workaround to be removed
				ds.ServiceComponentId IS NULL;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;

				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE r
					FROM
						tblComponentStatusRaw r LEFT JOIN
						tblServiceComponentDataSheet ds ON
							r.ServiceComponentId = ds.ServiceComponentId AND
							ds.EffectiveTo IS NULL
					WHERE
                        r.ServiceComponentId NOT IN (2244,2245) AND -- BtSportApp workaround to be removed
						ds.ServiceComponentId IS NULL;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In ServiceComponentId Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In ServiceComponentId Check INSERT'));
			END IF;

			/* INVALID CHANGE Check */
			INSERT INTO
				tblComponentStatusQuarantine
			SELECT
				r.ComponentId,
				r.ServiceId,
				r.ServiceComponentId,
				r.ComponentStatus,
				r.CreationDate,
				r.InsertDate,
				'INVALID DETAIL CHANGE'
			FROM
				tblComponentStatusRaw r INNER JOIN
				tblComponentStatus s ON
					r.ComponentId = s.ComponentId AND
					s.EffectiveTo IS NULL
			WHERE
				r.CreationDate <> s.CreationDate;

			IF @@error_count = 0 THEN
				SELECT ROW_COUNT() INTO vRowCount;

				IF vRowCount > 0 THEN
					SET vAffected = vAffected + vRowCount;

					DELETE r
					FROM
						tblComponentStatusRaw r INNER JOIN
						tblComponentStatus s ON
							r.ComponentId = s.ComponentId AND
							s.EffectiveTo IS NULL
					WHERE
						r.CreationDate <> s.CreationDate;

					IF @@error_count > 0 THEN
						SET vError = TRUE;
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In DETAIL CHANGE Check DELETE'));
					END IF;
				END IF;
			ELSE
				SET vError = TRUE;
				CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In DETAIL CHANGE Check INSERT'));
			END IF;

			/* Log completion of Quarantine processing */
			IF vError = TRUE THEN
				CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Completed with Error. Rows Affected: ', vAffected));
			ELSE
				IF vAffected <> 0 THEN
					CALL DataWarehouse.ipLogWarn (CONCAT(vInfoText, 'Completed. Rows Affected: ', vAffected));
				ELSE
					CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Completed.'));
				END IF;

				BEGIN
					/* Variables */
					DECLARE vInfoText	VARCHAR(32) DEFAULT 'LoadComponentStatus: ';
					DECLARE vUpdated,
						vInserted	INT UNSIGNED DEFAULT 0;

					CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Started'));

					UPDATE
						tblComponentStatus s INNER JOIN
						tblComponentStatusRaw r ON
								s.ComponentId = r.ComponentId
					SET
						s.EffectiveTo = FROM_DAYS(TO_DAYS(r.InsertDate) - 1)
					WHERE
						s.EffectiveTo IS NULL AND
						(r.ServiceId <> s.ServiceId OR
						r.ServiceComponentId <> s.ServiceComponentId OR
						r.ComponentStatus <> s.ComponentStatus);

					IF @@error_count = 0 THEN
						SELECT ROW_COUNT() INTO vUpdated;

						INSERT
							tblComponentStatus
								(ComponentId,
								ServiceId,
								ServiceComponentId,
								ComponentStatus,
								CreationDate,
								EffectiveFrom,
								EffectiveTo)
						SELECT
							r.ComponentId,
							r.ServiceId,
							r.ServiceComponentId,
							r.ComponentStatus,
							STR_TO_DATE(r.CreationDate, '%Y-%m-%d'),
							r.InsertDate AS EffectiveFrom,
							NULL AS EffectiveTo
						FROM
							tblComponentStatusRaw r LEFT JOIN
							tblComponentStatus s ON
								r.ComponentId = s.ComponentId
						WHERE
							s.ComponentId IS NULL OR
							s.EffectiveTo = FROM_DAYS(TO_DAYS(r.InsertDate) - 1);

						IF @@error_count = 0 THEN
							SELECT ROW_COUNT() INTO vInserted;
						ELSE
							SET vError = TRUE;
							CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In INSERT'));
						END IF;
                                                
					ELSE
						SET vError = TRUE;
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In UPDATE'));
					END IF;

					/* Log completion */
					IF vError = TRUE THEN
						CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Completed with Error. Rows Updated: ', vUpdated, ' Rows Inserted: ', vInserted));
					ELSE
						/* Data imported ok so empty the Raw table */
						TRUNCATE TABLE tblComponentStatusRaw;
						CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Completed. Rows Updated: ', vUpdated, ' Rows Inserted: ', vInserted));
					END IF;
				END;

			END IF;
		END;
	END IF;

	/* Log completion */
	SET vParams = ' ';
/*      SET vParams = CONCAT('Output Parameters: ', 'p1 = ', p1...)  */

	IF vError = TRUE THEN
		CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Completed with Error. ', vParams));
	ELSE
		CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Completed. ', vParams));
	END IF;
END; //

DELIMITER ;

