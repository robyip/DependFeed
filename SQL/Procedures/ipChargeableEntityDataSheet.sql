USE DataWarehouse;

DROP PROCEDURE IF EXISTS ipChargeableEntityDataSheet;

DELIMITER //

CREATE PROCEDURE ipChargeableEntityDataSheet ()
SQL SECURITY INVOKER
MODIFIES SQL DATA
DETERMINISTIC
COMMENT 'Update tblChargeableEntityDataSheet'
/*******************************************************************************
*
* Object:   Procedure
* Name:     ipChargeableEntityDataSheet
*
* Input Parameters
*
* Modification History
* 2011-07-07    jreadman    Created
* 2013-10-08    jreadman    Added BtSport Rgu
* 2015-01-13    iroberts    Added PlusnetTv Rgu
* 2015-12-10    iroberts    Replace SubComponentCategory with SubComponentId
* 2016-11-09    alangridge  Migrated
* 2017-07-18    jreadman    Added BtSportApp Rgu
*
*******************************************************************************/

BEGIN
  /* Variables */
  DECLARE vInfoText     VARCHAR(32) DEFAULT 'ipChargeableEntityDataSheet: ';
  DECLARE vParams       VARCHAR(100);
  DECLARE vError        BOOLEAN DEFAULT FALSE;
  DECLARE vInserted     INT UNSIGNED DEFAULT 0;

  /* Handlers */

  /* Initialise */
  SET vParams = ' ';
  CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Started ', vParams));

  /* Procedure Body */
  INSERT
   tblChargeableEntityDataSheet (
    ChargeableEntityId,
    ProductCategory,
    ProductSubCategory,
    ProductName)
  SELECT
   ce.ChargeableEntityId,
   CASE
    WHEN pds.access_type IS NOT NULL THEN
     CASE
      WHEN pds.access_type IN ('Broadband', 'SDSL') THEN 'Rgu'
      WHEN pds.access_type IN ('Metered Dial', 'Unmetered Dial') THEN 'Rgu'
      WHEN pds.access_type = 'Services' THEN 'AdditionalService'
      WHEN pds.access_type = 'Reseller Account' THEN 'Other'
      ELSE pds.access_type
     END
    WHEN cc.ChargeCategory IS NOT NULL THEN
     CASE
      WHEN cc.ChargeCategory  IN ('INTERNET_CONNECTION','WLR','YOUVIEW_TV','BT_SPORT_APP') AND sc.Handle = 'SUBSCRIPTION' THEN 'Rgu'
      WHEN cc.ChargeCategory = 'GENERIC_COMPONENT' AND ds.Category = 'BTSPORT' AND sc.Handle = 'SUBSCRIPTION' THEN 'Rgu'
      WHEN cc.ChargeCategory IN ('INTERNET_CONNECTION','WLR','YOUVIEW_TV','BT_SPORT_APP') AND sc.Handle <> 'SUBSCRIPTION' THEN 'AdditionalFeature'
      WHEN cc.ChargeCategory = 'PLUSTALK' AND sc.Handle = 'SUBSCRIPTION' THEN 'AdditionalService'
      WHEN cc.ChargeCategory = 'PLUSTALK' AND sc.Handle <> 'SUBSCRIPTION' THEN 'AdditionalFeature'
      WHEN cc.ChargeCategory = 'ENHANCED_CARE' OR sc.Handle = 'BbScopeEuSupport' THEN 'AdditionalFeature'
      WHEN cc.ChargeCategory IN ('SECURITY', 'PARENTAL_CONTROL', 'GENERIC_COMPONENT') AND ds.Category = 'SECURITY' THEN 'AdditionalService'
      WHEN cc.ChargeCategory IN ('STATIC_IP', 'GENERIC_COMPONENT') AND ds.Category = 'STATIC_IP' THEN 'AdditionalFeature'
      WHEN sc.Handle = 'SUBSCRIPTION' AND ds.Category = 'PAYH' THEN 'AdditionalService'
      WHEN cc.ChargeCategory = 'GENERIC_COMPONENT' AND ds.Category = 'ADSL' THEN 'AdditionalFeature'
      WHEN cc.ChargeCategory = 'GENERIC_COMPONENT' AND ds.Category = 'BIZPHONE' THEN 'Rgu'
      WHEN cc.ChargeCategory = 'GENERIC_COMPONENT' AND ds.Category = 'FTTC' THEN 'AdditionalFeature'
      WHEN cc.ChargeCategory = 'ADD_ON_PRODUCT_BUNDLE' THEN 'AdditionalFeature'
      WHEN cc.ChargeCategory = 'LEGACY' AND ds.Category = 'DOMAIN' THEN 'AdditionalService'
      WHEN cc.ChargeCategory = 'LEGACY' AND ds.Category = 'BILLING' THEN 'Other'
      WHEN cc.ChargeCategory = 'METRONET_EMAIL' THEN 'AdditionalService'
      WHEN sc.Handle IN ('PACK_OF_5_MAILBOXES', 'BUNDLED_JUSTMAIL_MAILBOXES') THEN 'AdditionalFeature'
      WHEN cc.ChargeCategory = 'MAX_PREMIUM' THEN 'AdditionalFeature'
      WHEN cc.ChargeCategory = 'GENERIC_COMPONENT' AND ds.Category IN ('ANNEXM', 'MAXPREMIUM') THEN 'AdditionalFeature'
      ELSE CONCAT_WS('*', ds.Category, cc.ChargeCategory, sc.Handle)
     END
    ELSE ''
   END,
   CASE
    WHEN pds.access_type IS NOT NULL THEN
     CASE
      WHEN pds.access_type IN ('Broadband', 'SDSL') THEN 'Broadband'
      WHEN pds.access_type IN ('Metered Dial', 'Unmetered Dial') THEN 'PaidDialup'
      WHEN pds.access_type = 'Services' THEN IF (pdl.ProductName LIKE '%mail%', 'Mail', 'Other')
      WHEN pds.access_type = 'Reseller Account' THEN 'Billing'
      ELSE pds.access_type
     END
    WHEN cc.ChargeCategory IS NOT NULL THEN
     CASE
      WHEN cc.ChargeCategory = 'INTERNET_CONNECTION' AND sc.Handle = 'SUBSCRIPTION' THEN 'Broadband'
      WHEN cc.ChargeCategory = 'INTERNET_CONNECTION' AND sc.Handle <> 'SUBSCRIPTION' THEN 'BroadbandFeature'
      WHEN cc.ChargeCategory = 'WLR' AND sc.Handle = 'SUBSCRIPTION' THEN 'Telco'
      WHEN cc.ChargeCategory = 'WLR' AND sc.Handle <> 'SUBSCRIPTION' THEN 'PhoneFeature'
      WHEN cc.ChargeCategory = 'YOUVIEW_TV' AND sc.Handle = 'SUBSCRIPTION' THEN 'PlusnetTv'
      WHEN cc.ChargeCategory = 'YOUVIEW_TV' AND sc.Handle <> 'SUBSCRIPTION' THEN 'TvPackage'
      WHEN cc.ChargeCategory = 'BT_SPORT_APP' AND sc.Handle = 'SUBSCRIPTION' THEN 'BtSportApp'
      WHEN cc.ChargeCategory = 'BT_SPORT_APP' AND sc.Handle <> 'SUBSCRIPTION' THEN 'BtSportAppFeature'
      WHEN cc.ChargeCategory = 'GENERIC_COMPONENT' AND ds.Category = 'BTSPORT' AND sc.Handle = 'SUBSCRIPTION' THEN 'BtSport'
      WHEN cc.ChargeCategory = 'PLUSTALK' AND sc.Handle = 'SUBSCRIPTION' THEN 'Voip'
      WHEN cc.ChargeCategory = 'PLUSTALK' AND sc.Handle <> 'SUBSCRIPTION' THEN 'VoipFeature'
      WHEN cc.ChargeCategory = 'ENHANCED_CARE' OR sc.Handle = 'BbScopeEuSupport' THEN 'EnhancedCare'
      WHEN cc.ChargeCategory IN ('SECURITY', 'PARENTAL_CONTROL', 'GENERIC_COMPONENT') AND ds.Category = 'SECURITY' THEN 'Security'
      WHEN cc.ChargeCategory IN ('STATIC_IP', 'GENERIC_COMPONENT') AND ds.Category = 'STATIC_IP' THEN 'StaticIp'
      WHEN sc.Handle = 'SUBSCRIPTION' AND ds.Category = 'PAYH' THEN 'Hosting'
      WHEN cc.ChargeCategory = 'GENERIC_COMPONENT' AND ds.Category = 'ADSL' THEN 'ServiceExperience'
      WHEN cc.ChargeCategory = 'GENERIC_COMPONENT' AND ds.Category = 'BIZPHONE' THEN 'Telco'
      WHEN cc.ChargeCategory = 'GENERIC_COMPONENT' AND ds.Category = 'FTTC' THEN 'Fttc'
      WHEN cc.ChargeCategory = 'ADD_ON_PRODUCT_BUNDLE' THEN 'Other'
      WHEN cc.ChargeCategory = 'LEGACY' AND ds.Category = 'DOMAIN' THEN 'Domain'
      WHEN cc.ChargeCategory = 'LEGACY' AND ds.Category = 'BILLING' THEN 'Billing'
      WHEN cc.ChargeCategory = 'METRONET_EMAIL' THEN 'Mail'
      WHEN sc.Handle IN ('PACK_OF_5_MAILBOXES', 'BUNDLED_JUSTMAIL_MAILBOXES') THEN 'Mailbox'
      WHEN cc.ChargeCategory = 'MAX_PREMIUM' THEN 'SupplierService'
      WHEN cc.ChargeCategory = 'GENERIC_COMPONENT' AND ds.Category IN ('ANNEXM', 'MAXPREMIUM') THEN 'SupplierService'
      ELSE CONCAT_WS('*', ds.Category, cc.ChargeCategory, sc.Handle)
     END
    ELSE ''
   END,
   COALESCE(REPLACE(pdl.ProductName, ',', ' '), CONCAT_WS(' ', REPLACE(ds.Name, ',', ' '), sc.Handle), '')
  FROM
   DataWarehouse.tblChargeableEntity ce LEFT JOIN
   DataWarehouse.tblProductDataLookup pdl ON
    ce.ProductId = pdl.ProductId AND
    pdl.EffectiveTo IS NULL LEFT JOIN
   DataWarehouse.tblProductDataSheet pds ON
    ce.ProductId = pds.product_id AND
    pds.product_superceded_date IS NULL LEFT JOIN
   DataWarehouse.tblServiceComponentDataSheet ds ON
    ce.ServiceComponentId = ds.ServiceComponentId AND
    ds.EffectiveTo IS NULL LEFT JOIN
   DataWarehouse.tblChargeableComponent cc ON
     ds.ServiceComponentId = cc.ServiceComponentId LEFT JOIN
   DataWarehouse.tblSubComponentDataSheet sc ON
     ce.SubComponentId = sc.SubComponentId LEFT JOIN
   DataWarehouse.tblChargeableEntityDataSheet ceds ON
     ce.ChargeableEntityId = ceds.ChargeableEntityId
  WHERE
   ceds.ChargeableEntityId IS NULL AND
   IFNULL(sc.Handle, '') NOT IN ('WlrLineRent', 'LINE_RENTAL_SAVER', 'METRONET_MAILBOX') AND
   IFNULL(ds.Category, '') <> 'LEGACY' AND
   IFNULL(pdl.ProductName, '') <> '[delete me]';

  IF @@error_count = 0 THEN
    SELECT ROW_COUNT() INTO vInserted;
  ELSE
    SET vError = TRUE;
    CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Error in tblChargeableEntityDataSheet INSERT'));
  END IF;

  /* Log completion */
  IF vError = TRUE THEN
    CALL DataWarehouse.ipLogError (CONCAT(vInfoText, 'Completed with Error. Inserted: ', vInserted));
  ELSE
    CALL DataWarehouse.ipLogInfo (CONCAT(vInfoText, 'Completed. Inserted: ', vInserted));
  END IF;
END; //

DELIMITER ;
