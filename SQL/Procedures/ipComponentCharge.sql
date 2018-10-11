USE DataWarehouse;

DROP PROCEDURE IF EXISTS ipComponentCharge;

DELIMITER //

CREATE PROCEDURE ipComponentCharge ()
SQL SECURITY INVOKER
MODIFIES SQL DATA
DETERMINISTIC
COMMENT 'Process component charge raw data'
/*******************************************************************************
*
* Object:   Procedure
* Name:     ipComponentCharge
*
* Input Parameters
*
* Modification History
* 2010-12-16    jreadman    Created
* 2011-03-04    jreadman    ServiceComponentId RANGE check
* 2011-06-29    jreadman    RANGE check, ChargePence can be 0
* 2015-12-10    iroberts    Add SubComponentId
* 2016-08-05    alangridge  Migrated
*
*******************************************************************************/

BEGIN
  /* Variables */
  DECLARE vInfoText     VARCHAR(32) DEFAULT 'ipComponentCharge: ';
  DECLARE vParams       VARCHAR(100);
  DECLARE vError        BOOLEAN DEFAULT FALSE;
  DECLARE vRawDataSets  INT DEFAULT 0;

  /* Handlers */

  /* Initialise */
  SET vParams = ' ';
/*  SET vParams = CONCAT('Input Parameters: ', 'p1 = ', p1...) */
  CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Started ', vParams));

  /* Procedure Body */
  SELECT COUNT(DISTINCT InsertDate) INTO vRawDataSets
  FROM tblComponentChargeRaw;

  IF @@error_count > 0 OR vRawDataSets <> 1 THEN
    SET vError = TRUE;
    CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error in tblComponentChargeRaw. vRawDataSets = ', IFNULL(vRawDataSets, 0)));
  ELSE
    BEGIN
      /* Variables */
      DECLARE vInfoText  VARCHAR(32) DEFAULT 'ComponentChargeQuarantine: ';
      DECLARE vAffected,
              vRowCount  INT UNSIGNED DEFAULT 0;

      CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Started'));

      /* INVALID NULL Check */
      INSERT INTO
        tblComponentChargeQuarantine
      SELECT
        ServiceComponentId,
        TariffId,
        SubComponentId,
        PricePlan,
        PaymentFrequency,
        DurationMonths,
        FollowOnTariffId,
        ChargePence,
        IsAvailable,
        EndDate,
        InsertDate,
        'INVALID NULL'
      FROM
        tblComponentChargeRaw
      WHERE
        ServiceComponentId IS NULL OR
        PaymentFrequency IS NULL OR
        ChargePence IS NULL OR
        IsAvailable IS NULL OR
        InsertDate IS NULL;

      IF @@error_count = 0 THEN
        SELECT ROW_COUNT() INTO vRowCount;
        IF vRowCount > 0 THEN
          SET vAffected = vAffected + vRowCount;

          DELETE FROM
            tblComponentChargeRaw
          WHERE
            ServiceComponentId IS NULL OR
            PaymentFrequency IS NULL OR
            ChargePence IS NULL OR
            IsAvailable IS NULL OR
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
      TRUNCATE TABLE tblComponentChargeDuplicate;

      IF @@error_count = 0 THEN
        INSERT INTO tblComponentChargeDuplicate
        SELECT ServiceComponentId, TariffId
         FROM tblComponentChargeRaw
        GROUP BY ServiceComponentId, TariffId
        HAVING COUNT(*) > 1;

        IF @@error_count = 0 THEN
          INSERT INTO
            tblComponentChargeQuarantine
          SELECT
            r.ServiceComponentId,
            r.TariffId,
            r.SubComponentId,
            r.PricePlan,
            r.PaymentFrequency,
            r.DurationMonths,
            r.FollowOnTariffId,
            r.ChargePence,
            r.IsAvailable,
            r.EndDate,
            r.InsertDate,
            'INVALID DUPLICATES'
          FROM
            tblComponentChargeRaw r INNER JOIN
            tblComponentChargeDuplicate d ON
              r.ServiceComponentId = d.ServiceComponentId AND
              r.TariffId <=> d.TariffId;

          IF @@error_count = 0 THEN
            SELECT ROW_COUNT() INTO vRowCount;

            IF vRowCount > 0 THEN
              SET vAffected = vAffected + vRowCount;

              DELETE r
              FROM
                tblComponentChargeRaw r INNER JOIN
                tblComponentChargeQuarantine q ON
                  r.SubComponentInstanceId = q.SubComponentInstanceId AND
                  r.TariffId <=> d.TariffId AND
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

      /* INVALID DATE AND TIME Check */
      INSERT INTO
        tblComponentChargeQuarantine
      SELECT
        ServiceComponentId,
        TariffId,
        SubComponentId,
        PricePlan,
        PaymentFrequency,
        DurationMonths,
        FollowOnTariffId,
        ChargePence,
        IsAvailable,
        EndDate,
        InsertDate,
        'INVALID DATE/TIME'
      FROM
        tblComponentChargeRaw
      WHERE
        (EndDate IS NOT NULL AND STR_TO_DATE(EndDate, '%Y-%m-%d') IS NULL) OR
        STR_TO_DATE(InsertDate, '%Y-%m-%d') IS NULL;

      IF @@error_count = 0 THEN
        SELECT ROW_COUNT() INTO vRowCount;

        IF vRowCount > 0 THEN
          SET vAffected = vAffected + vRowCount;

          DELETE FROM
            tblComponentChargeRaw
          WHERE
            (EndDate IS NOT NULL AND STR_TO_DATE(EndDate, '%Y-%m-%d') IS NULL) OR
            STR_TO_DATE(InsertDate, '%Y-%m-%d') IS NULL;

          IF @@error_count > 0 THEN
            SET vError = TRUE;
            CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In DATE/TIME Check DELETE'));
          END IF;
        END IF;
      ELSE
        SET vError = TRUE;
        CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In DATE/TIME Check INSERT'));
      END IF;

      /* INVALID RANGE Check */
      INSERT INTO
        tblComponentChargeQuarantine
      SELECT
        ServiceComponentId,
        TariffId,
        SubComponentId,
        PricePlan,
        PaymentFrequency,
        DurationMonths,
        FollowOnTariffId,
        ChargePence,
        IsAvailable,
        EndDate,
        InsertDate,
        'INVALID RANGE'
      FROM
        tblComponentChargeRaw
      WHERE
        ServiceComponentId < 1 OR
        ServiceComponentId > 65534 OR
        TariffId < 1 OR
        TariffId > 65534 OR
        SubComponentId < 1 OR
        SubComponentId > 32766 OR
        DurationMonths < 0 OR
        DurationMonths > 254 OR
        FollowOnTariffId < 1 OR
        FollowOnTariffId > 65534 OR
        ChargePence < 0 OR
        ChargePence > 4294967294 OR
        IsAvailable < 0 OR
        IsAvailable > 1 OR
        LENGTH(PricePlan) > 20 OR
        LENGTH(PaymentFrequency) > 20;

      IF @@error_count = 0 THEN
        SELECT ROW_COUNT() INTO vRowCount;

        IF vRowCount > 0 THEN
          SET vAffected = vAffected + vRowCount;

          DELETE FROM
            tblComponentChargeRaw
          WHERE
            ServiceComponentId < 1 OR
            ServiceComponentId > 65534 OR
            TariffId < 1 OR
            TariffId > 65534 OR
            SubComponentId < 1 OR
            SubComponentId > 32766 OR
            DurationMonths < 1 OR
            DurationMonths > 254 OR
            FollowOnTariffId < 1 OR
            FollowOnTariffId > 65534 OR
            ChargePence < 0 OR
            ChargePence > 4294967294 OR
            IsAvailable < 0 OR
            IsAvailable > 1 OR
            LENGTH(PricePlan) > 20 OR
            LENGTH(PaymentFrequency) > 20;

          IF @@error_count > 0 THEN
            SET vError = TRUE;
            CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In RANGE Check DELETE'));
          END IF;
        END IF;
      ELSE
        SET vError = TRUE;
        CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In RANGE Check INSERT'));
      END IF;

      /* INVALID CHANGE Check */
      INSERT INTO
        tblComponentChargeQuarantine
      SELECT
        r.ServiceComponentId,
        r.TariffId,
        r.SubComponentId,
        r.PricePlan,
        r.PaymentFrequency,
        r.DurationMonths,
        r.FollowOnTariffId,
        r.ChargePence,
        r.IsAvailable,
        r.EndDate,
        r.InsertDate,
        'INVALID CHANGE'
      FROM
        tblComponentChargeRaw r INNER JOIN
        tblComponentCharge s ON
          r.ServiceComponentId = s.ServiceComponentId AND
          r.TariffId = s.TariffId
      WHERE
        r.SubComponentId <> s.SubComponentId OR
        NOT(r.PricePlan <=> s.PricePlan) OR
        r.PaymentFrequency <> s.PaymentFrequency OR
        NOT(r.DurationMonths <=> s.DurationMonths);

      IF @@error_count = 0 THEN
        SELECT ROW_COUNT() INTO vRowCount;

        IF vRowCount > 0 THEN
          SET vAffected = vAffected + vRowCount;

          DELETE r
          FROM
            tblComponentChargeRaw r INNER JOIN
            tblComponentCharge s ON
              r.ServiceComponentId = s.ServiceComponentId AND
              r.TariffId = s.TariffId
          WHERE
            r.SubComponentId <> s.SubComponentId OR
            NOT(r.PricePlan <=> s.PricePlan) OR
            r.PaymentFrequency <> s.PaymentFrequency OR
            NOT(r.DurationMonths <=> s.DurationMonths);

          IF @@error_count > 0 THEN
            SET vError = TRUE;
            CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In CHANGE Check DELETE'));
          END IF;
        END IF;
      ELSE
        SET vError = TRUE;
        CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In CHANGE Check INSERT'));
      END IF;

      /* INVALID SubComponentId Check */
      INSERT INTO
        tblComponentChargeQuarantine
      SELECT
        r.ServiceComponentId,
        r.TariffId,
        r.SubComponentId,
        r.PricePlan,
        r.PaymentFrequency,
        r.DurationMonths,
        r.FollowOnTariffId,
        r.ChargePence,
        r.IsAvailable,
        r.EndDate,
        r.InsertDate,
        'INVALID SubComponentId'
      FROM
        tblComponentChargeRaw r LEFT JOIN
        tblSubComponentDataSheet sc ON r.SubComponentId = sc.SubComponentId
      WHERE
        r.SubComponentId IS NOT NULL AND
        sc.SubComponentId IS NULL;

      IF @@error_count = 0 THEN
        SELECT ROW_COUNT() INTO vRowCount;

        IF vRowCount > 0 THEN
          SET vAffected = vAffected + vRowCount;

          DELETE r
          FROM
            tblComponentChargeRaw r LEFT JOIN
            tblSubComponentDataSheet sc ON r.SubComponentId = sc.SubComponentId
          WHERE
            sc.SubComponentId IS NULL;

          IF @@error_count > 0 THEN
            SET vError = TRUE;
            CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In SubComponentId Check DELETE'));
          END IF;
        END IF;
      ELSE
        SET vError = TRUE;
        CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In SubComponentId Check INSERT'));
      END IF;

      /* INVALID ChargeableComponentId Check */
      INSERT INTO
        tblComponentChargeQuarantine
      SELECT
        r.ServiceComponentId,
        r.TariffId,
        r.SubComponentId,
        r.PricePlan,
        r.PaymentFrequency,
        r.DurationMonths,
        r.FollowOnTariffId,
        r.ChargePence,
        r.IsAvailable,
        r.EndDate,
        r.InsertDate,
        'INVALID ChargeableComponentId'
      FROM
        tblComponentChargeRaw r LEFT JOIN
        tblChargeableComponent cc ON r.ServiceComponentId = cc.ServiceComponentId
      WHERE
        cc.ServiceComponentId IS NULL;

      IF @@error_count = 0 THEN
        SELECT ROW_COUNT() INTO vRowCount;

        IF vRowCount > 0 THEN
          SET vAffected = vAffected + vRowCount;

          DELETE r
          FROM
            tblComponentChargeRaw r LEFT JOIN
            tblChargeableComponent cc ON r.ServiceComponentId = cc.ServiceComponentId
          WHERE
            cc.ServiceComponentId IS NULL;

          IF @@error_count > 0 THEN
            SET vError = TRUE;
            CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In ChargeableComponentId Check DELETE'));
          END IF;
        END IF;
      ELSE
        SET vError = TRUE;
        CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error In ChargeableComponentId Check INSERT'));
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
          DECLARE vInfoText  VARCHAR(32) DEFAULT 'LoadComponentCharge: ';
          DECLARE vUpdated,
                  vInserted  INT UNSIGNED DEFAULT 0;

          CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Started'));

          UPDATE
            tblComponentCharge s INNER JOIN
            tblComponentChargeRaw r ON
              s.ServiceComponentId = r.ServiceComponentId AND
              s.TariffId <=> r.TariffId
          SET
            s.EffectiveTo = DATE_SUB(r.InsertDate, INTERVAL 1 DAY)
          WHERE
            s.EffectiveTo IS NULL AND
            (NOT(r.FollowOnTariffId <=> s.FollowOnTariffId) OR
            r.ChargePence <> s.ChargePence OR
            r.IsAvailable <> s.IsAvailable OR
            NOT(r.EndDate <=> s.EndDate));

          IF @@error_count = 0 THEN
            SELECT ROW_COUNT() INTO vUpdated;

            INSERT
              tblComponentCharge
                (ServiceComponentId,
                 TariffId,
                 SubComponentId,
                 SubComponentCategory,
                 PricePlan,
                 PaymentFrequency,
                 DurationMonths,
                 FollowOnTariffId,
                 ChargePence,
                 IsAvailable,
                 EndDate,
                 EffectiveFrom,
                 EffectiveTo)
            SELECT
              r.ServiceComponentId,
              r.TariffId,
              r.SubComponentId,
              d.Handle,
              r.PricePlan,
              r.PaymentFrequency,
              r.DurationMonths,
              r.FollowOnTariffId,
              r.ChargePence,
              r.IsAvailable,
              STR_TO_DATE(r.EndDate, '%Y-%m-%d'),
              STR_TO_DATE(r.InsertDate, '%Y-%m-%d'),
              NULL AS EffectiveTo
            FROM
              tblComponentChargeRaw r LEFT JOIN
              tblSubComponentDataSheet d ON
                r.SubComponentId = d.SubComponentId LEFT JOIN
              tblComponentCharge s ON
                r.ServiceComponentId = s.ServiceComponentId AND
                s.TariffId <=> r.TariffId
            WHERE
              s.ServiceComponentId IS NULL OR
              s.EffectiveTo = DATE_SUB(r.InsertDate, INTERVAL 1 DAY);

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
            TRUNCATE TABLE tblComponentChargeRaw;
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
