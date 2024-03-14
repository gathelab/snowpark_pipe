# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import *
from snowflake.snowpark.functions import *

## from typing_extensions import Annotated
## from pathlib import Path
## import sys, typer, json, re, logging
## from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 

    ##-----------------------------------------------------------------------------------------------------------------------------
    ## Declaring variables
    stage_name = 'TPCDI_FILES'
    scale_factor = '10'
    file_name = 'all'
    overwrite = 'False'
    skip_upload = 'False'
    show= 'False'
	##-----------------------------------------------------------------------------------------------------------------------------
    
    ##-----------------------------------------------------------------------------------------------------------------------------
    ## method to get the stage details
    def get_stage_path(
            file_name:str
    ):
        return f"@TPCDI_STG.PUBLIC.{stage_name}/tmp/tpcdi/sf={scale_factor}/Batch{batch}/{file_name}"
    ##-----------------------------------------------------------------------------------------------------------------------------

    ##-----------------------------------------------------------------------------------------------------------------------------
    ## method to load the csv file as table
    def load_csv_table(
            schema: StructType,
            file_name: str,
            table_name: str,
            delimiter: str
    ):
        if file_name in ['*_audit.csv']:
            stage_path = f"@TPCDI_STG.PUBLIC.{stage_name}/tmp/tpcdi/sf={scale_factor}/Batch{batch}/Audit/"
            session \
            .read \
            .schema(schema) \
            .option("field_delimiter", delimiter) \
            .option("skip_header", 1) \
            .csv(stage_path) \
            .write.mode("append").save_as_table(table_name)
        else:
            stage_path = f"@TPCDI_STG.PUBLIC.{stage_name}/tmp/tpcdi/sf={scale_factor}/Batch{batch}/{file_name}"
            
            if file_name in ['BatchDate.txt', 'Prospect.csv', 'DailyMarket.txt', 'CashTransaction.txt', 'Trade.txt', 'WatchHistory.txt', 'HoldingHistory.txt', 'Account.txt', 'Customer.txt']:
                session \
                .read \
                .schema(schema) \
                .option("field_delimiter", delimiter) \
                .csv(stage_path) \
                .withColumn('batchid', lit(batch) ) \
                .write.mode("append").save_as_table(table_name)
            else:
                session \
                .read \
                .schema(schema) \
                .option("field_delimiter", delimiter) \
                .csv(stage_path) \
                .write.mode("append").save_as_table(table_name)
        
    ##-----------------------------------------------------------------------------------------------------------------------------

    ##-----------------------------------------------------------------------------------------------------------------------------
    ## Simplifies the DataFrame transformations for retrieving XML elements
    def get_xml_element(
            column:str,
            element:str,
            datatype:str,
            with_alias:bool = True
        ):
            new_element = get(xmlget(col(column), lit(element)), lit('$')).cast(datatype)
            if with_alias:
                return new_element.alias(element)
            else:
                return new_element
    ##-----------------------------------------------------------------------------------------------------------------------------

    ##-----------------------------------------------------------------------------------------------------------------------------
    ## Simplifies the DataFrame transformations for retrieving XML attributes
    def get_xml_attribute(
            column:str,
            attribute: str,
            datatype: str,
            with_alias:bool = True
    ):
        new_attribute = get(col(column), lit(f"@{attribute}")).cast(datatype)
        if with_alias:
            return new_attribute.alias(attribute)
        else:
            return new_attribute
    ##-----------------------------------------------------------------------------------------------------------------------------

    ##-----------------------------------------------------------------------------------------------------------------------------
    ## Simplifies the logic for constructing a phone number from multiple fields
    def get_phone_number(
            phone_id:str
    ):
        return iff( col(f"phone{phone_id}_local")=='' , lit(''), concat( iff( col(f"phone{phone_id}_ctry")=='', lit(''), concat(lit('+'), col(f"phone{phone_id}_ctry"),lit(' '))),iff( col(f"phone{phone_id}_area")=='', lit(''), concat(lit('('), col(f"phone{phone_id}_area"),lit(') '))), col(f"phone{phone_id}_local"),iff( col(f"phone{phone_id}_ext")=='', lit(''), col(f"phone{phone_id}_ext") ) )).alias(f"c_phone_{phone_id}") 

    ##-----------------------------------------------------------------------------------------------------------------------------
    ## Batch Validation

    def batch_completion(batch):

        session.sql (f"""
                    SELECT
                CURRENT_TIMESTAMP() as MessageDateAndTime,
                {batch} as BatchID,
                'Phase Complete Record' as MessageSource,
                'Batch Complete' as MessageText,
                'PCR' as MessageType,
                NULL as MessageData
        """).write.mode("append").save_as_table('TPCDI_WH.SILVER.DIMessages')

    ## ---------------------------------------------------------------------------------------------------------------------------
    ## Batch Validation

    def batch_validation(batch):

        batch_completion(batch)
        
        session.sql(f"""
                SELECT
                    CURRENT_TIMESTAMP() AS MessageDateAndTime,
                    CASE WHEN A.BatchID IS NULL THEN 0 ELSE A.BatchID END AS BatchID,
                    B.MessageSource,
                    B.MessageText,
                    'Validation' AS MessageType,
                   cast( B.MessageData as string) as MessageData
                FROM (SELECT {batch} AS BatchID) A
                CROSS JOIN 
                ( 				SELECT 	'DimAccount' AS MessageSource, 		'Row count' AS MessageText, count(*) AS MessageData		FROM	TPCDI_WH.SILVER.DimAccount
                    UNION		SELECT	'DimBroker' AS MessageSource, 		    'Row count' AS MessageText, count(*) AS MessageData		FROM	TPCDI_WH.SILVER.DimBroker
                    UNION		SELECT	'DimCompany' AS MessageSource, 		'Row count' AS MessageText, count(*) AS MessageData 	FROM	TPCDI_WH.SILVER.DimCompany
                    UNION		SELECT	'DimCustomer' AS MessageSource,		'Row count' AS MessageText, count(*) AS MessageData		FROM	TPCDI_WH.SILVER.DimCustomer
                    UNION		SELECT	'DimDate' AS MessageSource,			'Row count' AS MessageText,	count(*) AS MessageData		FROM	TPCDI_WH.BRONZE.DimDate
                    UNION		SELECT	'DimSecurity' AS MessageSource,		'Row count' AS MessageText,	count(*) AS MessageData		FROM	TPCDI_WH.SILVER.DimSecurity
                    UNION		SELECT	'DimTime' AS MessageSource,			'Row count' AS MessageText,	count(*) AS MessageData		FROM	TPCDI_WH.BRONZE.DimTime
                    UNION		SELECT	'DimTrade' AS MessageSource, 		    'Row count' AS MessageText, count(*) AS MessageData		FROM	TPCDI_WH.SILVER.DimTrade 
                    UNION		SELECT	'FactCashBalances' AS MessageSource,  'Row count' AS MessageText, count(*) AS MessageData 	FROM	TPCDI_WH.GOLD.FactCashBalances
    /****Test****/	UNION		SELECT	'FactHoldings' AS MessageSource, 	    'Row count' AS MessageText, count(*) AS MessageData	    FROM	TPCDI_WH.GOLD.FactHoldings
                    UNION		SELECT	'FactMarketHistory' AS MessageSource, 'Row count' AS MessageText, count(*) AS MessageData 	FROM	TPCDI_WH.GOLD.FactMarketHistory
    /****Test****/  UNION		SELECT	'FactWatches' AS MessageSource,		'Row count' AS MessageText,	count(*) AS MessageData		FROM    TPCDI_WH.GOLD.FactWatches
                    UNION		SELECT	'Financial' AS MessageSource,		'Row count' AS MessageText,	count(*) AS MessageData		FROM	TPCDI_WH.SILVER.Financial
                    UNION		SELECT	'Industry' AS MessageSource,		'Row count' AS MessageText,	count(*) AS MessageData		FROM	TPCDI_WH.BRONZE.Industry
                    UNION		SELECT	'Prospect' AS MessageSource,		'Row count' AS MessageText,	count(*) AS MessageData		FROM	TPCDI_WH.SILVER.Prospect
                    UNION		SELECT	'StatusType' AS MessageSource,		'Row count' AS MessageText,	count(*) AS MessageData		FROM	TPCDI_WH.BRONZE.StatusType
                    UNION		SELECT	'TaxRate' AS MessageSource,		'Row count' AS MessageText,	count(*) AS MessageData		FROM	TPCDI_WH.BRONZE.TaxRate
                    UNION		SELECT	'TradeType' AS MessageSource,		'Row count' AS MessageText,	count(*) AS MessageData		FROM	TPCDI_WH.BRONZE.TradeType        		
                    UNION		SELECT	'FactCashBalances' AS MessageSource,  'Row count joined' AS MessageText, 	count(*) AS MessageData		FROM	TPCDI_WH.GOLD.FactCashBalances f
                                    INNER JOIN TPCDI_WH.SILVER.DimAccount a ON		f.SK_AccountID = a.SK_ACCOUNTID
                                    INNER JOIN TPCDI_WH.SILVER.DimCustomer c ON		f.SK_CUSTOMERID = c.SK_CUSTOMERID
                                    INNER JOIN TPCDI_WH.SILVER.DimBroker b ON		a.SK_BROKERID = b.SK_BROKERID
                                    INNER JOIN TPCDI_WH.BRONZE.DimDate d ON			f.SK_DateID = d.SK_DateID
                
                
                    UNION		SELECT	'FactHoldings' AS MessageSource,	'Row count joined' AS MessageText,	count(*) AS MessageData		
    /****Test****/                  FROM	TPCDI_WH.GOLD.FACTHOLDINGS f 
                                    INNER JOIN TPCDI_WH.SILVER.DimAccount a ON		f.SK_ACCOUNTID = a.SK_ACCOUNTID
                                    INNER JOIN TPCDI_WH.SILVER.DimCustomer c ON		f.SK_CUSTOMERID = c.SK_CUSTOMERID
                                    INNER JOIN TPCDI_WH.SILVER.DimBroker b ON		a.SK_BROKERID = b.SK_BROKERID
                                    INNER JOIN TPCDI_WH.BRONZE.DimDate d ON			f.SK_DATEID = d.SK_DateID
                                    INNER JOIN TPCDI_WH.BRONZE.DimTime t ON			f.SK_TIMEID = t.SK_TIMEID
                                    INNER JOIN TPCDI_WH.SILVER.DimCompany m ON		f.SK_COMPANYID = m.SK_COMPANYID
                                    INNER JOIN TPCDI_WH.SILVER.DimSecurity s ON		f.SK_SECURITYID = s.sk_securityid
                                    
                    UNION		SELECT	'FactMarketHistory' AS MessageSource,'Row count joined' AS MessageText,	count(*) AS MessageData		FROM	TPCDI_WH.GOLD.FactMarketHistory f
                                    INNER JOIN TPCDI_WH.BRONZE.DimDate d ON			f.SK_DateID = d.SK_DateID
                                    INNER JOIN TPCDI_WH.SILVER.DimCompany m ON		f.SK_COMPANYID = m.SK_COMPANYID
                                    INNER JOIN TPCDI_WH.SILVER.DimSecurity s ON		f.SK_SECURITYID = s.SK_SECURITYID
                
                    UNION        	SELECT	'FactWatches' AS MessageSource,		'Row count joined' AS MessageText,	count(*) AS MessageData		
    /****Test****/                  FROM	TPCDI_WH.GOLD.FACTWATCHES f
                                    INNER JOIN TPCDI_WH.SILVER.DimCustomer c ON		f.SK_CUSTOMERID = c.SK_CUSTOMERID
                                    INNER JOIN TPCDI_WH.BRONZE.DimDate dp ON		f.SK_DATEID_DATEPLACED = dp.SK_DateID
                                    INNER JOIN TPCDI_WH.SILVER
                                    .DimSecurity s ON		f.SK_SECURITYID = s.sk_securityid
                                    
                    /* Additional information used at Audit time */
                    UNION		SELECT	'DimCustomer' AS MessageSource,		'Inactive customers' AS MessageText, count(*) FROM	TPCDI_WH.SILVER.DimCustomer		
                                        WHERE 	 IsCurrent AND Status in ('Inactive')
                    UNION		SELECT	'FactWatches' AS MessageSource,		'Inactive watches' AS MessageText,	count(*)
     /****Test****/	                    FROM	TPCDI_WH.GOLD.FACTWATCHES 	
                                        WHERE	SK_DATEID_DATEREMOVED IS NOT NULL
                ) B
    
                UNION	
                    
                SELECT
                CURRENT_TIMESTAMP() AS MessageDateAndTime,
                CASE	WHEN C.BatchID IS NULL THEN 0	ELSE C.BatchID	END AS BatchID,
                D.MessageSource,
                D.MessageText,
                'Alert' AS MessageType,
                D.MessageData
                FROM (  SELECT	{batch} AS batchid) C
                CROSS JOIN
                (			
                               SELECT		'DimCustomer' MessageSource,	'Invalid customer tier' MessageText,	concat('C_ID = ', customerid,', C_TIER = ',	nvl(CAST(tier AS string), 'null')) MessageData
                            FROM
                            (
                                    SELECT	customerid, tier
                                    FROM	TPCDI_WH.SILVER.DimCustomer		WHERE	batchid = {batch}	AND (tier NOT IN (1, 2, 3) OR tier IS NULL)		QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid	ORDER BY ENDDATE DESC 
                                    ) = 1 
                            )
                    
                    UNION ALL	SELECT	DISTINCT	'DimCustomer' MessageSource,	'DOB out of range' MessageText,		concat('C_ID = ', customerid,', C_DOB = ', dob) MessageData
                                FROM	TPCDI_WH.SILVER.DimCustomer dc	JOIN TPCDI_WH.BRONZE.BatchDate bd	USING (batchid)		
                                WHERE	dc.batchid = {batch}	AND bd.batchid = {batch}	AND ( ( ( DATEADD(year, 100, to_date(dob)) < to_date(batchdate)) ) OR ( to_date(dob) > to_date(batchdate) ) ) 
                                
                    UNION ALL	
                                SELECT	DISTINCT	'DimTrade' MessageSource,	'Invalid trade commission' MessageText,	concat('T_ID = ', tradeid,', T_COMM = ', commission) MessageData
                                FROM	TPCDI_WH.SILVER.DimTrade 		
                                WHERE	batchid = {batch}	AND 
                                commission IS NOT NULL	AND commission > tradeprice * quantity
            
                    UNION ALL	SELECT	DISTINCT	'DimTrade' MessageSource,	'Invalid trade fee' MessageText, 		concat('T_ID = ', tradeid,', T_CHRG = ', fee) MessageData
                                FROM	TPCDI_WH.SILVER.DimTrade   	
                                WHERE   batchid = {batch}	AND 
                                fee IS NOT NULL		AND fee > tradeprice * quantity
                                
                   UNION ALL	SELECT	DISTINCT	'FactMarketHistory' MessageSource,	'No earnings for company' MessageText,	concat('SYMBOL = ',SYMBOL) MessageData
                                FROM	TPCDI_WH.GOLD.FactMarketHistory fmh	JOIN TPCDI_WH.SILVER.DimSecurity ds ON	ds.sk_securityid = fmh.sk_securityid	
                                WHERE	fmh.batchid = {batch}	AND (fmh.PERATIO IS NULL OR fmh.PERATIO = 0) 
                                
                     UNION ALL	SELECT	DISTINCT	'DimCompany' MessageSource,	'Invalid SPRating' MessageText,	concat('CO_ID = ', cik,', CO_SP_RATE = ', sp_rating) MessageData
                                FROM	(	SELECT	 trim(substring(line, 79, 10)) CIK, trim(substring(line, 95, 4)) sp_rating FROM	TPCDI_STG.BRONZE.FINWIRE   	
                                                        WHERE	rec_type = 'CMP'	) 
                                        WHERE	sp_rating IN ('AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC', 'CC', 'C', 'D', 'AA+', 'A+', 'BBB+', 'BB+', 'B+', 'CCC+', 'AA-', 'A-', 'BBB-', 'BB-', 'B-', 'CCC-') /**/
                ) D 

    """).write.mode("append").save_as_table('TPCDI_WH.SILVER.DIMessages')
    ##-----------------------------------------------------------------------------------------------------------------------------

    ## ---------------------------------------------------------------------------------------------------------------------------
    ## Automated audit Validation

    def auto_audit():
        session.sql(""" 
        
                SELECT	*	FROM
            (
            	SELECT
            		'Audit table batches' AS Test,
            		NULL AS Batch,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.BRONZE.Audit) = 3 AND ( SELECT max(BatchID) FROM TPCDI_WH.BRONZE.Audit) = 3 THEN 'OK' ELSE 'Not 3 batches' END AS RESULT,
            		'There must be audit data for 3 batches' AS Description
            UNION ALL
            	SELECT
            		'Audit table sources' AS Test,
            		NULL AS Batch,
            		CASE WHEN ( SELECT count(DISTINCT DataSet) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet IN (   'Batch', 'DimAccount', 'DimBroker', 'DimCompany', 'DimCustomer', 'DimSecurity', 'DimTrade', 'FactHoldings', 'FactMarketHistory', 'FactWatches', 'Financial', 'Generator', 'Prospect' ) ) = 13 THEN 'OK' ELSE 'Mismatch' END AS RESULT,
            		'There must be audit data for all data sets' AS Description
            UNION ALL
            	SELECT
            		'DImessages validation reports',
            		BatchID,
            		RESULT,
            		'Every batch must have a full set of validation reports'
            	FROM
            		(
            			SELECT
            				DISTINCT BatchID,
            				( CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DIMessages WHERE BatchID = a.BatchID AND MessageType = 'Validation') = 24 THEN 'OK' ELSE 'Validation checks not fully reported' END ) AS RESULT
            			FROM TPCDI_WH.SILVER.DIMessages a    ) o
            UNION ALL
            	SELECT
            		'DImessages batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.SILVER.DIMessages) = 4 AND ( SELECT max(BatchID) FROM TPCDI_WH.SILVER.DIMessages) = 3 THEN 'OK' ELSE 'Not 3 batches plus batch 0' END,
            		'Must have 3 distinct batches reported in DImessages'
            UNION ALL
            	SELECT
            		'DImessages Phase complete records',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageType = 'PCR') = 4 AND ( SELECT max(BatchID) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageType = 'PCR') = 3 THEN 'OK' ELSE 'Not 4 Phase Complete Records' END,
            		'Must have 4 Phase Complete records'
            UNION ALL
            	SELECT
            		'DImessages sources',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM ( SELECT DISTINCT MessageSource FROM TPCDI_WH.SILVER.DIMessages WHERE MessageType = 'Validation' AND MessageSource IN ('DimAccount', 'DimBroker', 'DimCustomer', 'DimDate', 'DimSecurity', 'DimTime', 'DimTrade', 'FactCashBalances', 'FactHoldings', 'FactMarketHistory', 'FactWatches', 'Financial', 'Industry', 'Prospect', 'StatusType', 'TaxRate', 'TradeType') ) a ) = 17 THEN 'OK' ELSE 'Mismatch' END,
            		'Messages must be present for all tables/transforms'
            UNION ALL
            	SELECT
            		'DImessages initial condition',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DIMessages WHERE BatchID = 0 AND MessageType = 'Validation' AND MessageData <> '0') = 0 THEN 'OK' ELSE 'Non-empty table in before Batch1' END,
            		'All DW tables must be empty before Batch1'
            		-- Checks against the DimBroker table. 
            UNION ALL    
            	SELECT
            		'DimBroker row count',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimBroker) = ( SELECT Value FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimBroker' AND ATTRIBUTE = 'HR_BROKERS') THEN 'OK' ELSE 'Mismatch' END,
            		'Actual row count matches Audit table'
            UNION ALL    
            	SELECT
            		'DimBroker distinct keys',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT SK_BrokerID) FROM TPCDI_WH.SILVER.DimBroker) = ( SELECT Value FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimBroker' AND ATTRIBUTE = 'HR_BROKERS') THEN 'OK' ELSE 'Not unique' END,
            		'All SKs are distinct'
            UNION ALL    
            	SELECT
            		'DimBroker BatchID',
            		1,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimBroker WHERE BatchID <> 1) = 0 THEN 'OK' ELSE 'Not batch 1' END,
            		'All rows report BatchID = 1'
            UNION ALL    
            	SELECT
            		'DimBroker IsCurrent',
            		NULL,
            		--CASE WHEN ( SELECT count(*) FROM TPCDI_STG.PUBLIC.DIMBROKER WHERE ! IsCurrent) = 0 THEN 'OK' ELSE 'Not current' END,
                    CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimBroker WHERE IsCurrent != TRUE) = 0 THEN 'OK' ELSE 'Not current' END,
            		'All rows have IsCurrent = 1'
            UNION ALL    
            	SELECT
            		'DimBroker EffectiveDate',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimBroker WHERE EffectiveDate <> '1950-01-01') = 0 THEN 'OK' ELSE 'Wrong date' END,
            		'All rows have Batch1 BatchDate as EffectiveDate'
            UNION ALL    
            	SELECT
            		'DimBroker EndDate',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimBroker WHERE EndDate <> '9999-12-31') = 0 THEN 'OK' ELSE 'Wrong date' END,
            		'All rows have end of time as EndDate'
            		--  
            		-- Checks against the DimAccount table.
            		--  
            UNION ALL
            	SELECT
            		'DimAccount row count',
            		1,
            		CASE	WHEN                                   
            			(SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount WHERE BatchID = 1) >
            				( SELECT Value FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND ATTRIBUTE = 'C_NEW' AND BatchID = 1) + 
            				( SELECT Value FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimAccount' AND ATTRIBUTE = 'CA_ADDACCT' AND BatchID = 1) +
            				( SELECT Value FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimAccount' AND ATTRIBUTE = 'CA_CLOSEACCT' AND BatchID = 1) + 
            				( SELECT Value FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimAccount' AND ATTRIBUTE = 'CA_UPDACCT' AND BatchID = 1) + 
            				( SELECT Value FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND ATTRIBUTE = 'C_UPDCUST' AND BatchID = 1) + 
            				( SELECT Value FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND ATTRIBUTE = 'C_INACT' AND BatchID = 1) - 
            				( SELECT Value FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND ATTRIBUTE = 'C_ID_HIST' AND BatchID = 1) - 
            				( SELECT Value FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimAccount' AND ATTRIBUTE = 'CA_ID_HIST' AND BatchID = 1)
            		THEN 'OK' ELSE 'Too few rows' END,
            		'Actual row count matches or exceeds Audit table minimum'
            		-- had to change to sum(value) instead of just value because of correlated subquery issue
            UNION ALL
            	SELECT
            		'DimAccount row count',
            		BatchID,
            		RESULT,
            		'Actual row count matches or exceeds Audit table minimum'
            	FROM
            		(SELECT
            			DISTINCT BatchID,
            			(CASE	WHEN 
                              (SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount WHERE BatchID = a.BatchID) >= 
                              (SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimAccount' AND ATTRIBUTE = 'CA_ADDACCT' AND BatchID = a.BatchID) +
                              (SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimAccount' AND ATTRIBUTE = 'CA_CLOSEACCT' AND BatchID = a.BatchID) +
                              (SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimAccount' AND ATTRIBUTE = 'CA_UPDACCT' AND BatchID = a.BatchID) -
                              (SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimAccount' AND ATTRIBUTE = 'CA_ID_HIST' AND BatchID = a.BatchID)
            				THEN 'OK'	 ELSE 'Too few rows' END
            			) AS RESULT
            		FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (2, 3)	) o
            UNION ALL
            	SELECT
            		'DimAccount distinct keys',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT SK_AccountID) FROM TPCDI_WH.SILVER.DimAccount) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount) THEN 'OK' ELSE 'Not unique' END,
            		'All SKs are distinct'
            		-- Three tests together check for validity of the EffectiveDate and EndDate handling:
            		--   'DimAccount EndDate' checks that effective and end dates line up
            		--   'DimAccount Overlap' checks that there are not records that overlap in time
            		--   'DimAccount End of Time' checks that every company has a final record that goes to 9999-12-31
            UNION ALL
            	SELECT
            		'DimAccount EndDate',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount a JOIN TPCDI_WH.SILVER.DimAccount b ON a.AccountID = b.AccountID AND a.EndDate = b.EffectiveDate) + 
            														( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount WHERE EndDate = '9999-12-31') 
            			THEN 'OK' ELSE 'Dates not aligned' END,
            		'EndDate of one record matches EffectiveDate of another, or the end of time'
            UNION ALL
            	SELECT
            		'DimAccount Overlap',
            		NULL,
            		CASE WHEN (SELECT count(*) 
            					FROM TPCDI_WH.SILVER.DimAccount a 
            					JOIN TPCDI_WH.SILVER.DimAccount b ON a.AccountID = b.AccountID AND a.SK_AccountID <> b.SK_AccountID AND a.EffectiveDate >= b.EffectiveDate AND a.EffectiveDate < b.EndDate
            					) = 0
            			THEN 'OK' ELSE 'Dates overlap' END,
            		'Date ranges do not overlap for a given Account'
            UNION ALL
            	SELECT
            		'DimAccount End of Time',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT AccountID) FROM TPCDI_WH.SILVER.DimAccount) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount WHERE EndDate = '9999-12-31') THEN 'OK' ELSE 'End of tome not reached' END,
            		'Every Account has one record with a date range reaching the end of time'
            UNION ALL
            	SELECT
            		'DimAccount consolidation',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount WHERE EffectiveDate = EndDate) = 0     THEN 'OK' ELSE 'Not consolidated' END,
            		'No records become effective and end on the same day'
            UNION ALL
            	SELECT
            		'DimAccount batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.SILVER.DimAccount) = 3 AND ( SELECT max(BatchID) FROM TPCDI_WH.SILVER.DimAccount) = 3 THEN 'OK' ELSE 'Mismatch' END,
            		'BatchID values must match Audit table'
            UNION ALL
            	--This Code wasn't working with such a nested correlated subquery
            	--select 'DimAccount EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
            	--    select distinct BatchID, (
            	--          case when (
            	--                select count(*) FROM TPCDI_WH.SILVER.DimAccount
            	--                where BatchID = a.BatchID and (
            	--                      EffectiveDate < (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
            	--                      EffectiveDate > (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID) )
            	--          ) = 0 
            	--          then 'OK' else 'Data out of range - see ticket #71' end
            	--    ) as Result
            	--    FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o
            	SELECT
            		'DimAccount EffectiveDate',
            		BatchID,
            		RESULT,
            		'All records from a batch have an EffectiveDate in the batch time window'
            	FROM
            		(	SELECT
            				DISTINCT BatchID,
            				( CASE	WHEN (	SELECT count(*) 
            								FROM ( SELECT BatchID, Date, ATTRIBUTE FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE IN ('FirstDay', 'LastDay')) aud 
            								JOIN TPCDI_WH.SILVER.DimAccount da ON da.batchid = aud.batchid AND ( (da.EffectiveDate < aud.date AND ATTRIBUTE = 'FirstDay') OR (da.EffectiveDate > aud.date AND ATTRIBUTE = 'LastDay') ) 
            								WHERE da.batchid = a.batchid AND aud.BatchID = a.BatchID
            	            			) = 0 
            	            THEN 'OK'	ELSE 'Data out of range - see ticket #71'	END ) AS RESULT
            			FROM TPCDI_WH.BRONZE.Audit a	WHERE	BatchID IN (1, 2, 3)	) o
            UNION ALL
            	SELECT
            		'DimAccount IsCurrent',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount WHERE EndDate = '9999-12-31' AND IsCurrent) 
            														+ ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount WHERE EndDate < '9999-12-31' AND IsCurrent != TRUE) THEN 'OK' ELSE 'Not current' END,
            		'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
            UNION ALL
            	SELECT
            		'DimAccount Status',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount WHERE Status NOT IN ('Active', 'Inactive')) = 0 THEN 'OK' ELSE 'Bad value' END,
            		'All Status values are valid'
            UNION ALL
            	SELECT
            		'DimAccount TaxStatus',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount WHERE BatchID = 1 AND TaxStatus NOT IN (0, 1, 2)) = 0 THEN 'OK' ELSE 'Bad value' END,
            		'All TaxStatus values are valid'
            UNION ALL
            	SELECT
            		'DimAccount SK_CustomerID',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount a JOIN TPCDI_WH.SILVER.DimCustomer c ON a.SK_CustomerID = c.SK_CustomerID AND c.EffectiveDate <= a.EffectiveDate AND a.EndDate <= c.EndDate) 
            			THEN 'OK' ELSE 'Bad join' END,
            		'All SK_CustomerIDs match a DimCustomer record with a valid date range'
            UNION ALL    
            	SELECT
            		'DimAccount SK_BrokerID',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimAccount a JOIN TPCDI_WH.SILVER.DimBroker c ON a.SK_BrokerID = c.SK_BrokerID AND c.EffectiveDate <= a.EffectiveDate AND a.EndDate <= c.EndDate) 
            			THEN 'OK' ELSE 'Bad join - spec problem with DimBroker EffectiveDate values' END,
            		'All SK_BrokerIDs match a broker record with a valid date range'
            UNION ALL
            	SELECT
            		'DimAccount inactive customers',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM 
            						( SELECT count(*) FROM ( SELECT * FROM TPCDI_WH.SILVER.DimCustomer WHERE Status = 'Inactive') c 
            							LEFT JOIN TPCDI_WH.SILVER.DimAccount a ON a.SK_CustomerID = c.SK_CustomerID 
            							WHERE a.Status = 'Inactive' GROUP BY c.SK_CustomerID HAVING count(*) < 1
            						) z 
            					) = 0 
            			THEN 'OK' ELSE 'Bad value' END,
            		'If a customer is inactive, the corresponding accounts must also have been inactive'
            		--  
            		-- Checks against the DimCustomer table.
            		--  
            		--had to add sum for scalar subquery
            UNION ALL
            	SELECT
            		'DimCustomer row count',
            		BatchID,
            		RESULT,
            		'Actual row count matches or exceeds Audit table minimum'
            	FROM
            		(
            		SELECT
            			DISTINCT BatchID,
            			CASE	WHEN
            				(SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE BatchID = a.BatchID) >=
            				(SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND ATTRIBUTE = 'C_NEW' AND BatchID = a.BatchID) + 
            				(SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND ATTRIBUTE = 'C_INACT' AND BatchID = a.BatchID) +
            				(SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND ATTRIBUTE = 'C_UPDCUST' AND BatchID = a.BatchID) -
            				(SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND ATTRIBUTE = 'C_ID_HIST' AND BatchID = a.BatchID)
            			THEN 'OK'	ELSE 'Too few rows'	END AS RESULT
            		FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)
            		) o
            UNION ALL
            	SELECT
            		'DimCustomer distinct keys',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT SK_CustomerID) FROM TPCDI_WH.SILVER.DimCustomer) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer) THEN 'OK' ELSE 'Not unique' END,
            		'All SKs are distinct'
            		-- Three tests together check for validity of the EffectiveDate and EndDate handling:
            		--   'DimCustomer EndDate' checks that effective and end dates line up
            		--   'DimCustomer Overlap' checks that there are not records that overlap in time
            		--   'DimCustomer End of Time' checks that every company has a final record that goes to 9999-12-31
            UNION ALL
            	SELECT
            		'DimCustomer EndDate',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer a JOIN TPCDI_WH.SILVER.DimCustomer b ON a.CustomerID = b.CustomerID AND a.EndDate = b.EffectiveDate) + 
            														( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE EndDate = '9999-12-31') 
            			THEN 'OK' ELSE 'Dates not aligned' END,
            		'EndDate of one record matches EffectiveDate of another, or the end of time'
            UNION ALL
            	SELECT
            		'DimCustomer Overlap',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer a JOIN TPCDI_WH.SILVER.DimCustomer b ON a.CustomerID = b.CustomerID AND a.SK_CustomerID <> b.SK_CustomerID AND a.EffectiveDate >= b.EffectiveDate AND a.EffectiveDate < b.EndDate ) = 0 
            			THEN 'OK' ELSE 'Dates overlap' END,
            		'Date ranges do not overlap for a given Customer'
            UNION ALL
            	SELECT
            		'DimCustomer End of Time',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT CustomerID) FROM TPCDI_WH.SILVER.DimCustomer) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE EndDate = '9999-12-31') THEN 'OK' ELSE 'End of time not reached' END,
            		'Every Customer has one record with a date range reaching the end of time'
            UNION ALL
            	SELECT
            		'DimCustomer consolidation',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE EffectiveDate = EndDate) = 0    THEN 'OK' ELSE 'Not consolidated' END,
            		'No records become effective and end on the same day'
            UNION ALL
            	SELECT
            		'DimCustomer batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.SILVER.DimCustomer) = 3 AND ( SELECT max(BatchID) FROM TPCDI_WH.SILVER.DimCustomer) = 3 THEN 'OK' ELSE 'Mismatch' END,
            		'BatchID values must match Audit table'
            UNION ALL
            	SELECT
            		'DimCustomer IsCurrent',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE EndDate = '9999-12-31' AND IsCurrent) + 
            														( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE EndDate < '9999-12-31' AND IsCurrent != TRUE) 
            			THEN 'OK' ELSE 'Not current' END,
            		'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
            UNION ALL
            	--This Code wasn't working with such a nested correlated subquery and/or was returning more than one record in case statement.  Had to get it grouped together and counted in total
            	-- select 'DimCustomer EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
            	--       select distinct BatchID, (
            	--             case when (
            	--                   select count(*) FROM TPCDI_WH.SILVER.DimCustomer
            	--                   where BatchID = a.BatchID and (
            	--                         EffectiveDate < (select Date FROM TPCDI_WH.BRONZE.Audit a where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
            	--                         EffectiveDate > (select Date FROM TPCDI_WH.BRONZE.Audit a where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID) )
            	--             ) = 0 
            	--             then 'OK' else 'Data out of range' end
            	--       ) as Result
            	--       FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o
            	SELECT
            		'DimCustomer EffectiveDate',
            		BatchID,
            		RESULT,
            		'All records from a batch have an EffectiveDate in the batch time window'
            	FROM	(	SELECT	DISTINCT BatchID
            					, ( CASE WHEN ( SELECT count(*) FROM ( SELECT BatchID, Date, ATTRIBUTE FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE IN ('FirstDay', 'LastDay')) aud 
            															JOIN TPCDI_WH.SILVER.DimCustomer dc ON dc.batchid = aud.batchid AND ( (dc.EffectiveDate < aud.date AND ATTRIBUTE = 'FirstDay') OR (dc.EffectiveDate > aud.date AND ATTRIBUTE = 'LastDay')) 
            															WHERE dc.batchid = a.batchid AND aud.BatchID = a.BatchID ) = 0 
            							THEN 'OK' ELSE 'Data out of range' END) AS RESULT 
            				FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            UNION ALL
            	SELECT
            		'DimCustomer Status',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE Status NOT IN ('Active', 'Inactive')) = 0 THEN 'OK' ELSE 'Bad value' END,
            		'All Status values are valid'
            UNION ALL
            	-- HEAVILY modified this one since correlated subqueries 1) Have to be an aggregate and 2) Cannot use a non-equijoin
            	-- select 'DimCustomer inactive customers', BatchID, Result, 'Inactive customer count matches Audit table' from (
            	--       select distinct BatchID, case when
            	--             (select messageData FROM TPCDI_WH.SILVER.DIMessages where MessageType = 'Validation' and BatchID = a.BatchID and 'DimCustomer' = MessageSource and 'Inactive customers' = MessageText) = 
            	--             (select sum(Value) FROM TPCDI_WH.BRONZE.Audit where DataSet = 'DimCustomer' and BatchID <= a.BatchID and Attribute = 'C_INACT')         
            	--       then 'OK' else 'Mismatch' end as Result
            	--       FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o
            	SELECT
            		'DimCustomer inactive customers',
            		BatchID,
            		RESULT,
            		'Inactive customer count matches Audit table'
            	FROM
            		(SELECT	DISTINCT BatchID,
            				CASE WHEN ( SELECT sum(MessageData) FROM TPCDI_WH.SILVER.DIMessages 
            						WHERE MessageSource = 'DimCustomer' AND MessageType = 'Validation' AND MessageText = 'Inactive customers' AND BatchID = a.BatchID) = 
            						( SELECT sum(audit_batch_total) 
            							FROM ( SELECT batchid, sum(sum(Value)) OVER ( ORDER BY batchid) AS audit_batch_total 
            									FROM TPCDI_WH.BRONZE.Audit WHERE BatchID IN (1, 2, 3) AND DataSet = 'DimCustomer' AND ATTRIBUTE = 'C_INACT' GROUP BY batchid) 
            							WHERE BatchID = a.BatchID) 
            					THEN 'OK' ELSE 'Mismatch' END AS RESULT
            			FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)) o
            UNION ALL
            	SELECT
            		'DimCustomer Gender',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE Gender NOT IN ('M', 'F', 'U')) = 0 THEN 'OK' ELSE 'Bad value' END,
            		'All Gender values are valid'
            UNION ALL
            	--adding sum for value
            	SELECT
            		'DimCustomer age range alerts',
            		BatchID,
            		RESULT,
            		'Count of age range alerts matches audit table'
            	FROM	(	SELECT	DISTINCT BatchID,
            						CASE	WHEN	(SELECT count(*) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageType = 'Alert' AND BatchID = a.BatchID AND MessageText = 'DOB out of range') = 
            												( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND BatchID = a.BatchID AND ATTRIBUTE = 'C_DOB_TO') +
            												( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND BatchID = a.BatchID AND ATTRIBUTE = 'C_DOB_TY')
            						THEN 'OK'	ELSE 'Mismatch'	END AS RESULT
            				FROM TPCDI_WH.BRONZE.Audit a	WHERE	BatchID IN (1, 2, 3)	) o
            UNION ALL
            	--adding sum for value
            	SELECT
            		'DimCustomer customer tier alerts',
            		BatchID,
            		RESULT,
            		'Count of customer tier alerts matches audit table'
            	FROM	(	SELECT	DISTINCT BatchID,
            						CASE	WHEN	( SELECT count(*) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageType = 'Alert' AND BatchID = a.BatchID AND MessageText = 'Invalid customer tier') = 
            												( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimCustomer' AND BatchID = a.BatchID AND ATTRIBUTE = 'C_TIER_INV')
            						THEN 'OK'	ELSE 'Mismatch'	END AS RESULT
            				FROM TPCDI_WH.BRONZE.Audit a	WHERE	BatchID IN (1, 2, 3)) o
            UNION ALL
            	SELECT
            		'DimCustomer TaxID',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE TaxID NOT LIKE '___-__-____' ) = 0 THEN 'OK' ELSE 'Mismatch' END,
            		'TaxID values are properly formatted'
            UNION ALL
            	SELECT
            		'DimCustomer Phone1',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer 
            					WHERE Phone1 NOT LIKE '+1 (___) ___-____%' AND Phone1 NOT LIKE '(___) ___-____%' AND Phone1 NOT LIKE '___-____%' AND Phone1 <> '' AND Phone1 IS NOT NULL ) = 0 
            			THEN 'OK' ELSE 'Mismatch' END,
            		'Phone1 values are properly formatted'
            UNION ALL
            	SELECT
            		'DimCustomer Phone2',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer 
            					WHERE Phone2 NOT LIKE '+1 (___) ___-____%' AND Phone2 NOT LIKE '(___) ___-____%' AND Phone2 NOT LIKE '___-____%' AND Phone2 <> '' AND Phone2 IS NOT NULL ) = 0 
            			THEN 'OK' ELSE 'Mismatch' END,
            		'Phone2 values are properly formatted'
            UNION ALL
            	SELECT
            		'DimCustomer Phone3',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer 
            					WHERE Phone3 NOT LIKE '+1 (___) ___-____%' AND Phone3 NOT LIKE '(___) ___-____%' AND Phone3 NOT LIKE '___-____%' AND Phone3 <> '' AND Phone3 IS NOT NULL ) = 0 
            			THEN 'OK' ELSE 'Mismatch' END,
            		'Phone3 values are properly formatted'
            UNION ALL
            	SELECT
            		'DimCustomer Email1',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE Email1 NOT LIKE '_%.%@%.%' AND Email1 IS NOT NULL ) = 0 THEN 'OK' ELSE 'Mismatch' END,
            		'Email1 values are properly formatted'
            UNION ALL
            	SELECT
            		'DimCustomer Email2',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE Email2 NOT LIKE '_%.%@%.%' AND Email2 <> '' AND Email2 IS NOT NULL ) = 0 THEN 'OK' ELSE 'Mismatch' END,
            		'Email2 values are properly formatted'
            UNION ALL    
            	SELECT
            		'DimCustomer LocalTaxRate',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer c JOIN TPCDI_WH.BRONZE.TaxRate t ON c.LocalTaxRateDesc = t.TX_NAME AND c.LocalTaxRate = t.TX_RATE) 
            					AND ( SELECT count(DISTINCT LocalTaxRateDesc) FROM TPCDI_WH.SILVER.DimCustomer) > 300 
            			THEN 'OK' ELSE 'Mismatch' END,
            		'LocalTaxRateDesc and LocalTaxRate values are FROM TPCDI_WH.BRONZE.TaxRate table'
            UNION ALL    
            	SELECT
            		'DimCustomer NationalTaxRate',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer c JOIN TPCDI_WH.BRONZE.TaxRate t ON c.NationalTaxRateDesc = t.TX_NAME AND c.NationalTaxRate = t.TX_RATE) 
            					AND ( SELECT count(DISTINCT NationalTaxRateDesc) FROM TPCDI_WH.SILVER.DimCustomer) >= 9 -- Including the inequality for now, because the generated data is not sticking to national tax rates 
            			THEN 'OK' ELSE 'Mismatch' END,
            		'NationalTaxRateDesc and NationalTaxRate values are FROM TPCDI_WH.BRONZE.TaxRate table'
            UNION ALL    
            	SELECT
            		'DimCustomer demographic fields',
            		NULL,
            		CASE	WHEN	( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer c 
            								JOIN TPCDI_WH.SILVER.PROSPECT p 
            								ON upper(c.FirstName || c.LastName || c.AddressLine1 || COALESCE(c.AddressLine2, '') || c.PostalCode) = upper(p.FirstName || p.LastName || p.AddressLine1 || COALESCE(p.AddressLine2, '') || p.PostalCode) 
            								AND COALESCE(c.CreditRating, 0) = COALESCE(p.CreditRating, 0) 
            								AND COALESCE(c.NetWorth, 0) = COALESCE(p.NetWorth, 0) 
            								AND COALESCE(c.MarketingNameplate, '') = COALESCE(p.MarketingNameplate, '') 
            								AND c.IsCurrent 
            						) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCustomer WHERE AgencyID IS NOT NULL AND IsCurrent )
            				THEN 'OK' ELSE 'Mismatch' END,
            		'For current customer records that match Prospect records, the demographic fields also match'
            		--  
            		-- Checks against the DimSecurity table.
            		--  
            UNION ALL
            	-- Modified this one since correlated subqueries 1) Have to be an aggregate and 2) Cannot use a non-equijoin
            	-- select 'DimSecurity row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum' from (
            	--       select distinct BatchID, case when
            	--             cast((select MessageData FROM TPCDI_WH.SILVER.DIMessages where MessageType = 'Validation' and BatchID = a.BatchID and MessageSource = 'DimSecurity' and MessageText = 'Row count') as bigint) >= 
            	--             (select sum(Value) FROM TPCDI_WH.BRONZE.Audit where DataSet = 'DimSecurity' and Attribute = 'FW_SEC' and BatchID <= a.BatchID)
            	--       then 'OK' else 'Too few rows' end as Result
            	--       FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1)
            	-- ) o
            	SELECT
            		'DimSecurity row count',
            		BatchID,
            		RESULT,
            		'Actual row count matches or exceeds Audit table minimum'
            	FROM
            		(
            		SELECT
            			DISTINCT BatchID,
            			CASE WHEN CAST(( SELECT sum(MessageData) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageType = 'Validation' AND MessageSource = 'DimSecurity' AND MessageText = 'Row count' AND BatchID = a.BatchID) AS bigint) >= 
            									( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimSecurity' AND ATTRIBUTE = 'FW_SEC' AND BatchID = a.BatchID) -- This one was simpler since BatchID in (1) means only one batchid to compare to 
            					THEN 'OK' ELSE 'Too few rows' END AS RESULT FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1)) o
            UNION ALL    
            	SELECT
            		'DimSecurity distinct keys',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT SK_SecurityID) FROM TPCDI_WH.SILVER.DimSecurity) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity) THEN 'OK' ELSE 'Not unique' END,
            		'All SKs are distinct'
            /*		-- Three tests together check for validity of the EffectiveDate and EndDate handling:
            		--   'DimSecurity EndDate' checks that effective and end dates line up
            		--   'DimSecurity Overlap' checks that there are not records that overlap in time
            		--   'DimSecurity End of Time' checks that every company has a final record that goes to 9999-12-31*/
            UNION ALL    
            	SELECT
            		'DimSecurity EndDate',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity a JOIN TPCDI_WH.SILVER.DimSecurity b ON a.Symbol = b.Symbol AND a.EndDate = b.EffectiveDate) + ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity WHERE EndDate = '9999-12-31') 
            				THEN 'OK' ELSE 'Dates not aligned' END,
            		'EndDate of one record matches EffectiveDate of another, or the end of time'
            UNION ALL    
            	SELECT
            		'DimSecurity Overlap',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity a JOIN TPCDI_WH.SILVER.DimSecurity b ON a.Symbol = b.Symbol AND a.SK_SecurityID <> b.SK_SecurityID AND a.EffectiveDate >= b.EffectiveDate AND a.EffectiveDate < b.EndDate ) = 0 
            				THEN 'OK' ELSE 'Dates overlap' END,
            		'Date ranges do not overlap for a given company'
            UNION ALL    
            	SELECT
            		'DimSecurity End of Time',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT Symbol) FROM TPCDI_WH.SILVER.DimSecurity) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity WHERE EndDate = '9999-12-31') THEN 'OK' ELSE 'End of tome not reached' END,
            		'Every company has one record with a date range reaching the end of time'
            UNION ALL    
            	SELECT
            		'DimSecurity consolidation',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity WHERE EffectiveDate = EndDate) = 0    THEN 'OK' ELSE 'Not consolidated' END,
            		'No records become effective and end on the same day'
            UNION ALL    
            	SELECT
            		'DimSecurity batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.SILVER.DimSecurity) = 1 AND ( SELECT max(BatchID) FROM TPCDI_WH.SILVER.DimSecurity) = 1 THEN 'OK' ELSE 'Mismatch' END,
            		'BatchID values must match Audit table'
            UNION ALL    
            	SELECT
            		'DimSecurity IsCurrent',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity WHERE EndDate = '9999-12-31' AND IsCurrent) + 
            														( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity WHERE EndDate < '9999-12-31' AND IsCurrent != TRUE) 
            				THEN 'OK' ELSE 'Not current' END,
            		'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
            UNION ALL
            	--This Code wasn't working with such a nested correlated subquery and/or was returning more than one record in case statement.  Had to get it grouped together and counted in total
            	-- select 'DimSecurity EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
            	--       select distinct BatchID, (
            	--             case when (
            	--                   select count(*) FROM TPCDI_WH.SILVER.DimSecurity
            	--                   where BatchID = a.BatchID and (
            	--                 --added alias for audit and filter the same
            	--                         EffectiveDate < (select Date FROM TPCDI_WH.BRONZE.Audit a where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
            	--                         EffectiveDate > (select Date FROM TPCDI_WH.BRONZE.Audit a where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID))
            	--             ) = 0 
            	--             then 'OK' else 'Data out of range' end
            	--       ) as Result
            	--       FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o    
            	SELECT
            		'DimSecurity EffectiveDate',
            		BatchID,
            		RESULT,
            		'All records from a batch have an EffectiveDate in the batch time window'
            	FROM	(	SELECT 	DISTINCT BatchID, 
            						( CASE WHEN ( SELECT count(*) FROM ( SELECT BatchID, Date, ATTRIBUTE FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE IN ('FirstDay', 'LastDay')) aud 
            															JOIN TPCDI_WH.SILVER.DimSecurity dc	ON 	dc.batchid = aud.batchid 
            															AND ( (dc.EffectiveDate < aud.date AND ATTRIBUTE = 'FirstDay') OR (dc.EffectiveDate > aud.date AND ATTRIBUTE = 'LastDay')) 
            															WHERE dc.batchid = a.batchid AND aud.BatchID = a.BatchID 
            								) = 0 
            							THEN 'OK' ELSE 'Data out of range' END) AS RESULT 
            				FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            UNION ALL    
            	SELECT
            		'DimSecurity Status',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity WHERE Status NOT IN ('Active', 'Inactive')) = 0 THEN 'OK' ELSE 'Bad value' END,
            		'All Status values are valid'
            UNION ALL    
            	SELECT
            		'DimSecurity SK_CompanyID',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity a JOIN TPCDI_WH.SILVER.DimCompany c ON a.SK_CompanyID = c.SK_CompanyID AND c.EffectiveDate <= a.EffectiveDate AND a.EndDate <= c.EndDate) 
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_CompanyIDs match a DimCompany record with a valid date range'
            UNION ALL    
            	SELECT
            		'DimSecurity ExchangeID',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity WHERE ExchangeID NOT IN ('NYSE  ', 'NASDAQ', 'AMEX  ', 'PCX   ')) = 0 THEN 'OK' ELSE 'Bad value - see ticket #65' END,
            		'All ExchangeID values are valid'
            UNION ALL    
            	SELECT
            		'DimSecurity Issue',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimSecurity WHERE Issue NOT IN ('COMMON', 'PREF_A', 'PREF_B', 'PREF_C', 'PREF_D')) = 0 THEN 'OK' ELSE 'Bad value - see ticket #65' END,
            		'All Issue values are valid'
            		--  
            		-- Checks against the DimCompany table.
            		--  
            UNION ALL
            	SELECT
            		'DimCompany row count',
            		BatchID,
            		RESULT,
            		'Actual row count matches or exceeds Audit table minimum'
            	FROM
            		(	SELECT	DISTINCT BatchID,
            					CASE	WHEN	
            								--added sum(messagedata)
            								CAST(( SELECT sum(MessageData) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageType = 'Validation' AND BatchID = a.BatchID AND MessageSource = 'DimCompany' AND MessageText = 'Row count') AS bigint) <=
            								--added audit alias and filter
            								( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit a WHERE DataSet = 'DimCompany' AND ATTRIBUTE = 'FW_CMP' AND BatchID <= a.BatchID) 
            							THEN 'OK' ELSE 'Too few rows' END AS RESULT
            			FROM TPCDI_WH.BRONZE.Audit a	WHERE	BatchID IN (1, 2, 3)	) o
            UNION ALL 
            	SELECT
            		'DimCompany distinct keys',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT SK_CompanyID) FROM TPCDI_WH.SILVER.DimCompany) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany) THEN 'OK' ELSE 'Not unique' END,
            		'All SKs are distinct'
            		-- Three tests together check for validity of the EffectiveDate and EndDate handling:
            		--   'DimCompany EndDate' checks that effective and end dates line up
            		--   'DimCompany Overlap' checks that there are not records that overlap in time
            		--   'DimCompany End of Time' checks that every company has a final record that goes to 9999-12-31
            UNION ALL    
            	SELECT
            		'DimCompany EndDate',
            		NULL,
            CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany a JOIN TPCDI_WH.SILVER.DimCompany b ON a.CompanyID = b.CompanyID AND a.EndDate = b.EffectiveDate) + 
            												( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany WHERE EndDate = '9999-12-31') 
            		THEN 'OK' ELSE 'Dates not aligned' END, 
            		'EndDate of one record matches EffectiveDate of another, or the end of time'
            UNION ALL    
            	SELECT
            		'DimCompany Overlap',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany a JOIN TPCDI_WH.SILVER.DimCompany b ON a.CompanyID = b.CompanyID AND a.SK_CompanyID <> b.SK_CompanyID AND a.EffectiveDate >= b.EffectiveDate AND a.EffectiveDate < b.EndDate ) = 0 
            				THEN 'OK' ELSE 'Dates overlap' END,
            		'Date ranges do not overlap for a given company'
            UNION ALL    
            	SELECT
            		'DimCompany End of Time',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT CompanyID) FROM TPCDI_WH.SILVER.DimCompany) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany WHERE EndDate = '9999-12-31') THEN 'OK' ELSE 'End of tome not reached' END,
            		'Every company has one record with a date range reaching the end of time'
            UNION ALL    
            	SELECT
            		'DimCompany consolidation',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany WHERE EffectiveDate = EndDate) = 0     THEN 'OK' ELSE 'Not consolidated' END,
            		'No records become effective and end on the same day'
            UNION ALL    
            	SELECT
            		'DimCompany batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.SILVER.DimCompany) = 1 AND ( SELECT max(BatchID) FROM TPCDI_WH.SILVER.DimCompany) = 1 THEN 'OK' ELSE 'Mismatch' END,
            		'BatchID values must match Audit table'
            UNION ALL
            	--This Code wasn't working with such a nested correlated subquery and/or was returning more than one record in case statement.  Had to get it grouped together and counted in total
            	-- select 'DimCompany EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
            	--       select distinct BatchID, (
            	--             case when (
            	--                   select count(*) FROM TPCDI_WH.SILVER.DimCompany
            	--                   where BatchID = a.BatchID and (
            	--             --added audit alias 
            	--                         EffectiveDate < (select Date FROM TPCDI_WH.BRONZE.Audit a where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
            	--                         EffectiveDate > (select Date FROM TPCDI_WH.BRONZE.Audit a where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID))
            	--             ) = 0 
            	--             then 'OK' else 'Data out of range - see ticket #71' end
            	--       ) as Result
            	--       FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o    
            	SELECT
            		'DimCompany EffectiveDate',
            		BatchID,
            		RESULT,
            		'All records from a batch have an EffectiveDate in the batch time window'
            	FROM	(	SELECT DISTINCT BatchID, 
            						( CASE WHEN ( SELECT count(*) FROM ( SELECT BatchID, Date, ATTRIBUTE FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE IN ('FirstDay', 'LastDay')) aud 
            														JOIN TPCDI_WH.SILVER.DimCompany dc ON dc.batchid = aud.batchid AND ( (dc.EffectiveDate < aud.date AND ATTRIBUTE = 'FirstDay') OR (dc.EffectiveDate > aud.date AND ATTRIBUTE = 'LastDay')) 
            														WHERE dc.batchid = a.batchid AND aud.BatchID = a.BatchID 
            									) = 0 
            								THEN 'OK' ELSE 'Data out of range' END) AS RESULT 
            						FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            UNION ALL    
            	SELECT
            		'DimCompany Status',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany WHERE Status NOT IN ('Active', 'Inactive')) = 0 THEN 'OK' ELSE 'Bad value' END,
            		'All Status values are valid'
            UNION ALL    
            	SELECT
            		'DimCompany distinct names',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany a JOIN TPCDI_WH.SILVER.DimCompany b ON a.Name = b.Name AND a.CompanyID <> b.CompanyID ) = 0 THEN 'OK' ELSE 'Mismatch' END,
            		'Every company has a unique name'
            UNION ALL    
            	-- Curious, there are duplicate industry names in Industry table.  Should there be?  That's why the distinct stuff...
            	SELECT
            		'DimCompany Industry',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany WHERE Industry IN ( SELECT DISTINCT IN_NAME FROM TPCDI_WH.BRONZE.Industry)) THEN 'OK' ELSE 'Bad value' END,
            		'Industry values are from the Industry table'
            UNION ALL   
            	SELECT
            		'DimCompany SPrating',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany 
            						WHERE SPrating NOT IN ('AAA ','AA+ ','AA- ','A+  ','A-  ','AA  ','A   ','BBB ','BB- ','BB+ ','B+  ','B-  ','BB  ','B   ','BBB+','BBB-','CCC-','CCC+','CCC ','CC  ','C   ','D   ') AND SPrating IS NOT NULL 
            					) = 0 
            				THEN 'OK' ELSE 'Bad value' END,
            		'All SPrating values are valid'
            UNION ALL    
            	-- Right now we have blank (but not null) country names.  Should there be?
            	SELECT
            		'DimCompany Country',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimCompany WHERE Country NOT IN ( 'Canada                  ', 'United States of America', '                        ' )  AND Country IS NOT NULL ) = 0 THEN 'OK' ELSE 'Bad value' END,
            		'All Country values are valid'
            		--  
            		-- Checks against the Prospect table.
            		--  
            UNION ALL
            	SELECT
            		'Prospect SK_UpdateDateID',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.PROSPECT WHERE SK_RecordDateID < SK_UpdateDateID) = 0   THEN 'OK' ELSE 'Mismatch' END,
            		'SK_RecordDateID must be newer or same as SK_UpdateDateID'
            UNION ALL
            	SELECT
            		'Prospect SK_RecordDateID',
            		BatchID,
            		RESULT,
            		'All records from batch have SK_RecordDateID in or after the batch time window'
            	FROM	(	SELECT	DISTINCT BatchID,
            						(	CASE	WHEN (	SELECT count(*)	FROM TPCDI_WH.SILVER.PROSPECT p 
            											JOIN TPCDI_WH.BRONZE.DimDate ON SK_DateId = P.SK_RecordDateID 
            											JOIN TPCDI_WH.BRONZE.Audit _A ON DataSet = 'Batch' AND ATTRIBUTE = 'FirstDay' AND DateValue < Date AND P.BatchID = _A.BatchID
            							
            											--                where p.BatchID = a.BatchID and (
            											--                      (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = p.SK_RecordDateID) < 
            											--                      (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
            											--                )
            										) = 0
            							THEN 'OK'	ELSE 'Mismatch'	END	) AS RESULT
            				FROM TPCDI_WH.BRONZE.Audit a	WHERE	BatchID IN (1, 2, 3)	) o
            UNION ALL    
            	SELECT
            		'Prospect batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.SILVER.PROSPECT) = 3 AND ( SELECT max(BatchID) FROM TPCDI_WH.SILVER.PROSPECT) = 3 THEN 'OK' ELSE 'Mismatch' END,
            		'BatchID values must match Audit table'
            UNION ALL    
            	SELECT
            		'Prospect Country',
            		NULL,
            			-- For the tiny sample data it would be ( 'CANADA', 'USA' )
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.PROSPECT WHERE Country NOT IN ( 'Canada', 'United States of America' )	AND Country IS NOT NULL ) = 0 THEN 'OK' ELSE 'Bad value' END,
            		'All Country values are valid'
            UNION ALL    
            	SELECT
            		'Prospect MarketingNameplate',
            		NULL,
            		CASE	WHEN (
            						SELECT
            							sum(CASE WHEN (COALESCE(NetWorth, 0) > 1000000 OR COALESCE(Income, 0) > 200000) AND MarketingNameplate NOT LIKE '%HighValue%' THEN 1 ELSE 0 END) +
                         				sum(CASE WHEN (COALESCE(NumberChildren, 0) > 3 OR COALESCE(NumberCreditCards, 0) > 5) AND MarketingNameplate NOT LIKE '%Expenses%' THEN 1 ELSE 0 END) +
                         				sum(CASE WHEN (COALESCE(Age, 0) > 45) AND MarketingNameplate NOT LIKE '%Boomer%' THEN 1 ELSE 0 END) +
                         				sum(CASE WHEN (COALESCE(Income, 50000) < 50000 OR COALESCE(CreditRating, 600) < 600 OR COALESCE(NetWorth, 100000) < 100000) AND MarketingNameplate NOT LIKE '%MoneyAlert%' THEN 1 ELSE 0 END) +
                         				sum(CASE WHEN (COALESCE(NumberCars, 0) > 3 OR COALESCE(NumberCreditCards, 0) > 7) AND MarketingNameplate NOT LIKE '%Spender%' THEN 1 ELSE 0 END) +
                         				sum(CASE WHEN (COALESCE(Age, 25) < 25 AND COALESCE(NetWorth, 0) > 1000000) AND MarketingNameplate NOT LIKE '%Inherited%' THEN 1 ELSE 0 END) +
                           				sum(CASE WHEN COALESCE(MarketingNameplate, '') NOT IN (-- Technically, a few of these combinations cannot really happen
            			                        '', 'HighValue', 'Expenses', 'HighValue+Expenses', 'Boomer', 'HighValue+Boomer', 'Expenses+Boomer', 'HighValue+Expenses+Boomer', 'MoneyAlert', 'HighValue+MoneyAlert',
            			                        'Expenses+MoneyAlert', 'HighValue+Expenses+MoneyAlert', 'Boomer+MoneyAlert', 'HighValue+Boomer+MoneyAlert', 'Expenses+Boomer+MoneyAlert', 'HighValue+Expenses+Boomer+MoneyAlert',
            			                        'Spender', 'HighValue+Spender', 'Expenses+Spender', 'HighValue+Expenses+Spender', 'Boomer+Spender', 'HighValue+Boomer+Spender', 'Expenses+Boomer+Spender',
            			                        'HighValue+Expenses+Boomer+Spender', 'MoneyAlert+Spender', 'HighValue+MoneyAlert+Spender', 'Expenses+MoneyAlert+Spender', 'HighValue+Expenses+MoneyAlert+Spender',
            			                        'Boomer+MoneyAlert+Spender', 'HighValue+Boomer+MoneyAlert+Spender', 'Expenses+Boomer+MoneyAlert+Spender', 'HighValue+Expenses+Boomer+MoneyAlert+Spender', 'Inherited',
            			                        'HighValue+Inherited', 'Expenses+Inherited', 'HighValue+Expenses+Inherited', 'Boomer+Inherited', 'HighValue+Boomer+Inherited', 'Expenses+Boomer+Inherited',
            			                        'HighValue+Expenses+Boomer+Inherited', 'MoneyAlert+Inherited', 'HighValue+MoneyAlert+Inherited', 'Expenses+MoneyAlert+Inherited', 'HighValue+Expenses+MoneyAlert+Inherited',
            			                        'Boomer+MoneyAlert+Inherited', 'HighValue+Boomer+MoneyAlert+Inherited', 'Expenses+Boomer+MoneyAlert+Inherited', 'HighValue+Expenses+Boomer+MoneyAlert+Inherited',
            			                        'Spender+Inherited', 'HighValue+Spender+Inherited', 'Expenses+Spender+Inherited', 'HighValue+Expenses+Spender+Inherited', 'Boomer+Spender+Inherited',
            			                        'HighValue+Boomer+Spender+Inherited', 'Expenses+Boomer+Spender+Inherited', 'HighValue+Expenses+Boomer+Spender+Inherited', 'MoneyAlert+Spender+Inherited',
            			                        'HighValue+MoneyAlert+Spender+Inherited', 'Expenses+MoneyAlert+Spender+Inherited', 'HighValue+Expenses+MoneyAlert+Spender+Inherited', 'Boomer+MoneyAlert+Spender+Inherited',
            			                        'HighValue+Boomer+MoneyAlert+Spender+Inherited', 'Expenses+Boomer+MoneyAlert+Spender+Inherited', 'HighValue+Expenses+Boomer+MoneyAlert+Spender+Inherited') 
            			          				THEN 1 ELSE 0 END)
            						FROM TPCDI_WH.SILVER.PROSPECT
            					) = 0
            			THEN 'OK'	ELSE 'Bad value'	END,
            		'All MarketingNameplate values match the data'
            		--
            -- Checks against the FactWatches table.
            		--
            
            UNION ALL
            	SELECT
            		'FactWatches row count',
            		BatchID,
            		RESULT,
            		'Actual row count matches Audit table'
            	FROM	(	--add sum message data and sum value
            			SELECT
            					DISTINCT BatchID,
            					(
            						CASE WHEN	CAST(( SELECT sum(MessageData) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageSource = 'FactWatches' AND MessageType = 'Validation' AND MessageText = 'Row count' AND BatchID = a.BatchID) AS int) -	
            									CAST(( SELECT sum(MessageData) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageSource = 'FactWatches' AND MessageType = 'Validation' AND MessageText = 'Row count' AND BatchID = a.BatchID-1) AS int) =	
            									( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'FactWatches' AND ATTRIBUTE = 'WH_ACTIVE' AND BatchID = a.BatchID) 
            								THEN 'OK'	ELSE 'Mismatch'	END
            					) AS RESULT
            			FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            UNION ALL
            	SELECT
            		'FactWatches batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.GOLD.FACTWATCHES) = 3 AND ( SELECT max(BatchID) FROM TPCDI_WH.GOLD.FACTWATCHES) = 3 THEN 'OK' ELSE 'Mismatch' END,
            		'BatchID values must match Audit table'
            UNION ALL
            	-- HEAVILY modified this one since correlated subqueries 1) Have to be an aggregate and 2) Cannot use a non-equijoin
            	-- select 'FactWatches active watches', BatchID, Result, 'Actual total matches Audit table' from (
            	--       select distinct BatchID, case when
            	--             (select cast(MessageData as bigint) FROM TPCDI_WH.SILVER.DIMessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) +
            	--             (select cast(MessageData as bigint) FROM TPCDI_WH.SILVER.DIMessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Inactive watches' and BatchID = a.BatchID) =
            	--             (select sum(Value) FROM TPCDI_WH.BRONZE.Audit where DataSet = 'FactWatches' and Attribute = 'WH_RECORDS' and BatchID <= a.BatchID)
            	--       then 'OK' else 'Mismatch' end as Result
            	--       FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o
            	SELECT
            		'FactWatches active watches',
            		BatchID,
            		RESULT,
            		'Actual total matches Audit table'
            	FROM	(	SELECT DISTINCT BatchID
            					, CASE WHEN	( SELECT CAST(sum(MessageData) AS bigint) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageSource = 'FactWatches' AND MessageType = 'Validation' AND MessageText = 'Row count' AND BatchID = a.BatchID) +
            									( SELECT CAST(sum(MessageData) AS bigint) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageSource = 'FactWatches' AND MessageType = 'Validation' AND MessageText = 'Inactive watches' AND BatchID = a.BatchID) =
            									( SELECT sum(audit_batch_total) FROM ( SELECT batchid, sum(sum(Value)) OVER ( ORDER BY batchid) AS audit_batch_total FROM TPCDI_WH.BRONZE.Audit WHERE BatchID IN (1, 2, 3) AND DataSet = 'FactWatches' AND ATTRIBUTE = 'WH_RECORDS' GROUP BY batchid) WHERE BatchID = a.BatchID)
            							THEN 'OK' ELSE 'Mismatch' END AS RESULT 
            				FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            UNION ALL    
            	SELECT
            		'FactWatches SK_CustomerID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID_DatePlaced) <= c.EndDate )
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID_DatePlaced)		
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTWATCHES) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTWATCHES a	JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = A.SK_DateID_DatePlaced JOIN TPCDI_WH.SILVER.DimCustomer c ON a.SK_CustomerID = c.SK_CustomerID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate) 
            				THEN 'OK' ELSE 'Bad join' END,		
            		'All SK_CustomerIDs match a DimCustomer record with a valid date range'
            UNION ALL    
            	SELECT
            		'FactWatches SK_SecurityID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID_DatePlaced)
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID_DatePlaced) <= c.EndDate )
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTWATCHES) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTWATCHES a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_DateID_DatePlaced JOIN TPCDI_WH.SILVER.DimSecurity c ON a.SK_SecurityID = c.SK_SecurityID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate) 
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_SecurityIDs match a DimSecurity record with a valid date range'
            UNION ALL
            	-- Modified because of 1) too far nested correlated subqueries and 2) ANY correlated subqueries need aggregation (added first())
            	-- select 'FactWatches date check', BatchID, Result, 'All SK_DateID_ values are in the correct batch time window' from (
            	--       select distinct BatchID, (
            	--             case when (
            	--                   select count(*) FROM TPCDI_WH.GOLD.FACTWATCHES w
            	--                   where w.BatchID = a.BatchID and (
            	--                         w.SK_DateID_DateRemoved is null and (
            	--                               (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = w.SK_DateID_DatePlaced) > 
            	--                               (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
            	--                               or
            	--                               (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = w.SK_DateID_DatePlaced) < 
            	--                               (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
            	--                         ) or 
            	--                         w.SK_DateID_DateRemoved is not null and (
            	--                               (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = w.SK_DateID_DateRemoved) > 
            	--                               (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
            	--                               or
            	--                               (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = w.SK_DateID_DateRemoved) < 
            	--                               (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
            	--                               or
            	--                               SK_DateID_DatePlaced > SK_DateID_DateRemoved
            	--                         )
            	--                   )
            	--             ) = 0
            	--             then 'OK' else 'Mismatch' end
            	--       ) as Result
            	--       FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o 
            	SELECT
            		'FactWatches date check',
            		BatchID,
            		RESULT,
            		'All SK_DateID_ values are in the correct batch time window'
            	FROM	(	SELECT	DISTINCT BatchID,
            						(	CASE	WHEN (	SELECT count(*) FROM TPCDI_WH.GOLD.FACTWATCHES w
            											WHERE	w.BatchID = a.BatchID	
            												AND (w.SK_DateID_DateRemoved IS NULL 
            														AND (		(SELECT MIN(DateValue) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = w.SK_DateID_DatePlaced) > 
            																	( SELECT MIN(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE = 'LastDay' AND BatchID = w.BatchID)
            																OR	( SELECT MIN(DateValue) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = w.SK_DateID_DatePlaced) < 
            																	( SELECT MIN(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE = 'FirstDay' AND BatchID = w.BatchID)
            															  )
            														OR w.SK_DateID_DateRemoved IS NOT NULL
            														AND (		( SELECT MIN(DateValue) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = w.SK_DateID_DateRemoved) > 
            																	( SELECT MIN(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE = 'LastDay' AND BatchID = w.BatchID)
            																OR	( SELECT MIN(DateValue) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = w.SK_DateID_DateRemoved) < 
            																	( SELECT MIN(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE = 'FirstDay' AND BatchID = w.BatchID)
            																OR	SK_DateID_DatePlaced > SK_DateID_DateRemoved
            															)
            													)
            										) = 0
            									THEN 'OK' ELSE 'Mismatch' END
            						) AS RESULT
            				FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            		--  
            		-- Checks against the DimTrade table.
            		--  
            UNION ALL
            	-- HEAVILY modified this one since correlated subqueries 1) Have to be an aggregate and 2) Cannot use a non-equijoin
            	-- select 'DimTrade row count', BatchID, Result, 'Actual total matches Audit table' from (
            	--       select distinct BatchID, case when
            	--             (select MessageData FROM TPCDI_WH.SILVER.DIMessages where MessageSource = 'DimTrade' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID)  =
            	--             (select sum(Value) FROM TPCDI_WH.BRONZE.Audit where DataSet = 'DimTrade' and Attribute = 'T_NEW' and BatchID <= a.BatchID)
            	--       then 'OK' else 'Mismatch' end as Result
            	--       FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o
            	SELECT
            		'DimTrade row count',
            		BatchID,
            		RESULT,
            		'Actual total matches Audit table'
            	FROM
            		(
            		SELECT
            			DISTINCT BatchID,
            			CASE
            				WHEN	( SELECT sum(MessageData) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageSource = 'DimTrade' AND MessageType = 'Validation' AND MessageText = 'Row count' AND BatchID = a.BatchID) =
            						( SELECT sum(audit_batch_total) 
            							FROM ( SELECT batchid, sum(sum(Value)) OVER ( ORDER BY batchid) AS audit_batch_total FROM TPCDI_WH.BRONZE.Audit WHERE BatchID IN (1, 2, 3) AND DataSet = 'DimTrade' AND ATTRIBUTE = 'T_NEW' GROUP BY batchid) 
            							WHERE BatchID = a.BatchID)
            				THEN 'OK'	ELSE 'Mismatch'	END AS RESULT
            		FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            UNION ALL
            	SELECT
            		'DimTrade canceled trades',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade WHERE Status = 'Canceled') = ( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimTrade' AND ATTRIBUTE = 'T_CanceledTrades') THEN 'OK' ELSE 'Mismatch' END,
            		'Actual row counts matches Audit table'
            UNION ALL
            	SELECT
            		'DimTrade commission alerts',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageType = 'Alert' AND messageText = 'Invalid trade commission') = ( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimTrade' AND ATTRIBUTE = 'T_InvalidCommision') THEN 'OK' ELSE 'Mismatch' END,
            		'Actual row counts matches Audit table'
            UNION ALL
            	SELECT
            		'DimTrade charge alerts',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageType = 'Alert' AND messageText = 'Invalid trade fee') = ( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'DimTrade' AND ATTRIBUTE = 'T_InvalidCharge') THEN 'OK' ELSE 'Mismatch' END,
            		'Actual row counts matches Audit table'
            UNION ALL
            	SELECT
            		'DimTrade batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.SILVER.DimTrade) = 3 AND ( SELECT max(BatchID) FROM TPCDI_WH.SILVER.DimTrade) = 3 THEN 'OK' ELSE 'Mismatch' END,
            		'BatchID values must match Audit table'
            UNION ALL
            	SELECT
            		'DimTrade distinct keys',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT TradeID) FROM TPCDI_WH.SILVER.DimTrade) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade) THEN 'OK' ELSE 'Not unique' END,
            		'All keys are distinct'
            UNION ALL
            	SELECT
            		'DimTrade SK_BrokerID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_CreateDateID JOIN TPCDI_WH.SILVER.DimBroker c ON a.SK_BrokerID = c.SK_BrokerID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate) 
            			THEN 'OK' ELSE 'Bad join' END,
            		'All SK_BrokerIDs match a DimBroker record with a valid date range'
            UNION ALL    
            	SELECT
            		'DimTrade SK_CompanyID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_CreateDateID JOIN TPCDI_WH.SILVER.DimCompany c ON a.SK_CompanyID = c.SK_CompanyID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate)
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_CompanyIDs match a DimCompany record with a valid date range'
            UNION ALL    
            	SELECT
            		'DimTrade SK_SecurityID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_CreateDateID JOIN TPCDI_WH.SILVER.DimSecurity c ON a.SK_SecurityID = c.SK_SecurityID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate)
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_SecurityIDs match a DimSecurity record with a valid date range'
            UNION ALL    
            	SELECT
            		'DimTrade SK_CustomerID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_CreateDateID JOIN TPCDI_WH.SILVER.DimCustomer c ON a.SK_CustomerID = c.SK_CustomerID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate)
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_CustomerIDs match a DimCustomer record with a valid date range'
            UNION ALL    
            	SELECT
            		'DimTrade SK_AccountID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade) = ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_CreateDateID JOIN TPCDI_WH.SILVER.DimAccount c ON a.SK_AccountID = c.SK_AccountID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate)
            		THEN 'OK' ELSE 'Bad join' END,
            		'All SK_AccountIDs match a DimAccount record with a valid date range'
            UNION ALL
            	-- Modified because of 1) too far nested correlated subqueries and 2) ANY correlated subqueries need aggregation (added first())
            	-- select 'DimTrade date check', BatchID, Result, 'All SK_DateID values are in the correct batch time window' from (
            	--       select distinct BatchID, (
            	--             case when (
            	--                   select count(*) FROM TPCDI_WH.SILVER.DimTrade w
            	--                   where w.BatchID = a.BatchID and (
            	--                         w.SK_CloseDateID is null and (
            	--                               (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = w.SK_CreateDateID) > 
            	--                               (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
            	--                               or
            	--                               (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = w.SK_CreateDateID) < 
            	--                               (case when w.Type like 'Limit%'  /* Limit trades can have create dates earlier than the current Batch date, but not earlier than Batch1's first date */
            	--                                               then (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = 1) 
            	--                                               else (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) end)
            	--                         ) or 
            	--                         w.SK_CloseDateID is not null and (
            	--                               (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = w.SK_CloseDateID) > 
            	--                               (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
            	--                               or
            	--                               (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = w.SK_CloseDateID) < 
            	--                               (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
            	--                               or
            	--                               SK_CloseDateID < SK_CreateDateID
            	--                         )
            	--                   )
            	--             ) = 0
            	--             then 'OK' else 'Mismatch' end
            	--       ) as Result
            	--       FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o 
            
            	SELECT
            		'DimTrade date check',
            		BatchID,
            		RESULT,
            		'All SK_DateID values are in the correct batch time window'
            	FROM	(	/* Limit trades can have create dates earlier than the current Batch date, but not earlier than Batch1's first date */
            				SELECT	DISTINCT BatchID,
            						(	CASE	WHEN (	SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade w
            												WHERE	w.BatchID = a.BatchID
            														AND (	w.SK_CloseDateID IS NULL
            																AND (		( SELECT MIN(DateValue) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = w.SK_CreateDateID) > ( SELECT MIN(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE = 'LastDay' AND BatchID = w.BatchID)
            																		OR	( SELECT	 MIN(DateValue) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = w.SK_CreateDateID) < 
            																			(CASE WHEN w.Type LIKE 'Limit%' THEN ( SELECT MIN(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE = 'FirstDay' AND BatchID = 1) ELSE ( SELECT MIN(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE = 'FirstDay' AND BatchID = w.BatchID) END)
            																	)
            																OR w.SK_CloseDateID IS NOT NULL
            																AND (		( SELECT MIN(DateValue) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = w.SK_CloseDateID) > ( SELECT MIN(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE = 'LastDay' AND BatchID = w.BatchID)
            																		OR	( SELECT	MIN(DateValue) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = w.SK_CloseDateID) < ( SELECT MIN(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND ATTRIBUTE = 'FirstDay' AND BatchID = w.BatchID)
            																		OR	SK_CloseDateID < SK_CreateDateID
            																	)
            															)
            										  ) = 0
            									THEN 'OK' ELSE 'Mismatch' END 
            						) AS RESULT 
            				FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            UNION ALL
            	SELECT
            		'DimTrade Status',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade WHERE Status NOT IN ( 'Canceled', 'Pending', 'Submitted', 'Active', 'Completed' ) ) = 0 THEN 'OK' ELSE 'Bad value' END,
            		'All Trade Status values are valid'
            UNION ALL
            	SELECT
            		'DimTrade Type',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.DimTrade WHERE TYPE NOT IN ( 'Market Buy', 'Market Sell', 'Stop Loss', 'Limit Sell', 'Limit Buy' ) ) = 0 THEN 'OK' ELSE 'Bad value' END,
            		'All Trade Type values are valid'
            		--  
            		-- Checks against the Financial table.
            		--  
            UNION ALL    
            	SELECT
            		'Financial row count',
            		NULL,
            		CASE
            			WHEN ( SELECT MessageData FROM TPCDI_WH.SILVER.DIMessages WHERE MessageSource = 'Financial' AND MessageType = 'Validation' AND MessageText = 'Row count' AND BatchID = 1) =
            					( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Financial' AND ATTRIBUTE = 'FW_FIN')
            			THEN 'OK'	ELSE 'Mismatch'	END,
            		'Actual row count matches Audit table'
            UNION ALL    
            	SELECT
            		'Financial SK_CompanyID',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.Financial) = ( SELECT count(*) FROM TPCDI_WH.SILVER.Financial a JOIN TPCDI_WH.SILVER.DimCompany c ON a.SK_CompanyID = c.SK_CompanyID ) THEN 'OK' ELSE 'Bad join' END,
            		'All SK_CompanyIDs match a DimCompany record'
            UNION ALL    
            	SELECT
            		'Financial FI_YEAR',
            		NULL,
            		CASE	WHEN (	( SELECT count(*) FROM TPCDI_WH.SILVER.Financial WHERE FI_YEAR < YEAR(( SELECT TO_DATE(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND BatchID = 1 AND ATTRIBUTE = 'FirstDay'))) + 
            						( SELECT count(*) FROM TPCDI_WH.SILVER.Financial WHERE FI_YEAR > YEAR(( SELECT TO_DATE(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND BatchID = 1 AND ATTRIBUTE = 'LastDay')))
            					 ) = 0
            				THEN 'OK' ELSE 'Bad Year' END,
            		'All Years are within Batch1 range'
            UNION ALL    
            	SELECT
            		'Financial FI_QTR',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.Financial WHERE FI_QTR NOT IN ( 1, 2, 3, 4 ) ) = 0 THEN 'OK' ELSE 'Bad Qtr' END,
            		'All quarters are in ( 1, 2, 3, 4 )'
            UNION ALL    
            	SELECT
            		'Financial FI_QTR_START_DATE',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.Financial WHERE FI_YEAR <> YEAR(FI_QTR_START_DATE) OR MONTH(FI_QTR_START_DATE) <> (FI_QTR-1)* 3 + 1 OR DAY(FI_QTR_START_DATE) <> 1 ) = 0 THEN 'OK' ELSE 'Bad date' END,
            		'All quarters start on correct date'
            UNION ALL    
            	SELECT
            		'Financial EPS',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.SILVER.Financial WHERE Round(FI_NET_EARN / FI_OUT_BASIC, 2)- FI_BASIC_EPS NOT BETWEEN -0.4 AND 0.4 OR Round(FI_NET_EARN / FI_OUT_DILUT, 2) - FI_DILUT_EPS NOT BETWEEN -0.4 AND 0.4 OR Round(FI_NET_EARN / FI_REVENUE, 2) - FI_MARGIN NOT BETWEEN -0.4 AND 0.4 ) = 0 THEN 'OK' ELSE 'Bad EPS' END,
            		'Earnings calculations are valid'
            		--  
            		-- Checks against the FactMarketHistory table.
            		--  
            UNION ALL
            	SELECT
            		'FactMarketHistory row count',
            		BatchID,
            		RESULT,
            		'Actual row count matches Audit table'
            	FROM	(	--added sum(messageData) and sum(value)
            				SELECT
            					DISTINCT BatchID,
            					(	CASE
            							WHEN	CAST(( SELECT sum(MessageData) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageSource = 'FactMarketHistory' AND MessageType = 'Validation' AND MessageText = 'Row count' AND BatchID = a.BatchID) AS int) -
            									CAST(( SELECT sum(MessageData) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageSource = 'FactMarketHistory' AND MessageType = 'Validation' AND MessageText = 'Row count' AND BatchID = a.BatchID-1) AS int) =
            									( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'FactMarketHistory' AND ATTRIBUTE = 'DM_RECORDS' AND BatchID = a.BatchID)
            							THEN 'OK' ELSE 'Mismatch' END
            					) AS RESULT
            				FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)
            			) o
            UNION ALL
            	SELECT
            		'FactMarketHistory batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.GOLD.FactMarketHistory) = 3 AND ( SELECT max(BatchID) FROM TPCDI_WH.GOLD.FactMarketHistory) = 3 THEN 'OK' ELSE 'Mismatch' END,
            		'BatchID values must match Audit table'
            UNION ALL    
            	SELECT
            		'FactMarketHistory SK_CompanyID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FactMarketHistory) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FactMarketHistory a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_DateID JOIN TPCDI_WH.SILVER.DimCompany c ON a.SK_CompanyID = c.SK_CompanyID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate) 
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_CompanyIDs match a DimCompany record with a valid date range'
            UNION ALL    
            	SELECT
            		'FactMarketHistory SK_SecurityID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FactMarketHistory) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FactMarketHistory a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_DateID JOIN TPCDI_WH.SILVER.DimSecurity c ON a.SK_SecurityID = c.SK_SecurityID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate) 
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_SecurityIDs match a DimSecurity record with a valid date range'
            UNION ALL
            	-- Modified because of 1) too far nested correlated subqueries and 2) ANY correlated subqueries need aggregation (added first())
            	-- select 'FactMarketHistory SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
            	--       select distinct BatchID, (
            	--             case when (
            	--                   (select count(*) FROM TPCDI_WH.GOLD.FactMarketHistory m where m.BatchID = a.BatchID and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = m.SK_DateID) < (select Date-1 day FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'FirstDay')) + 
            	--                   (select count(*) FROM TPCDI_WH.GOLD.FactMarketHistory m where m.BatchID = a.BatchID and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = m.SK_DateID) >= (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'LastDay'))
            	--             ) = 0
            	--             then 'OK' else 'Bad Date' end
            	--       ) as Result
            	--       FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o   
            	SELECT
            		'FactMarketHistory SK_DateID',
            		BatchID,
            		RESULT,
            		'All dates are within batch date range'
            	FROM	(	SELECT DISTINCT BatchID, 
            						( CASE	WHEN (	( SELECT count(*) FROM TPCDI_WH.GOLD.FactMarketHistory m 
            												WHERE m.BatchID = a.BatchID 
            												AND ( SELECT min(TO_DATE(DateValue)) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = m.SK_DateID) < ( SELECT min(TO_DATE(Date)) -1 DAY FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND BatchID = m.BatchID AND ATTRIBUTE = 'FirstDay')
            										) + 
            										( SELECT count(*) FROM TPCDI_WH.GOLD.FactMarketHistory m 
            												WHERE m.BatchID = a.BatchID 
            												AND ( SELECT min(TO_DATE(DateValue)) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = m.SK_DateID) >= ( SELECT min(TO_DATE(Date)) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND BatchID = m.BatchID AND ATTRIBUTE = 'LastDay')
            										)
            									 ) = 0
            								THEN 'OK' ELSE 'Bad Date' END
            						) AS RESULT
            				FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            UNION ALL
            	SELECT
            		'FactMarketHistory relative dates',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FactMarketHistory WHERE FiftyTwoWeekLow > DayLow OR DayLow > ClosePrice OR ClosePrice > DayHigh OR DayHigh > FiftyTwoWeekHigh ) = 0 THEN 'OK' ELSE 'Bad Date' END,
            		'52-week-low <= day_low <= close_price <= day_high <= 52-week-high'
            		--  
            		-- Checks against the FactHoldings table.
            		--  
            UNION ALL
            	SELECT
            		'FactHoldings row count',
            		BatchID,
            		RESULT,
            		'Actual row count matches Audit table'
            	FROM	(	--added sum for messagedata and value
            				SELECT DISTINCT BatchID,
            						( CASE WHEN	CAST(( SELECT sum(MessageData) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageSource = 'FactHoldings' AND MessageType = 'Validation' AND MessageText = 'Row count' AND BatchID = a.BatchID) AS int) -
            									CAST(( SELECT sum(MessageData) FROM TPCDI_WH.SILVER.DIMessages WHERE MessageSource = 'FactHoldings' AND MessageType = 'Validation' AND MessageText = 'Row count' AND BatchID = a.BatchID-1) AS int) =
            									( SELECT sum(Value) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'FactHoldings' AND ATTRIBUTE = 'HH_RECORDS' AND BatchID = a.BatchID)
            								THEN 'OK' ELSE 'Mismatch' END
            				      ) AS RESULT
            				FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            UNION ALL
            	SELECT
            		'FactHoldings batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BATCHID) FROM TPCDI_WH.GOLD.FACTHOLDINGS) = 3 AND ( SELECT max(BATCHID) FROM TPCDI_WH.GOLD.FACTHOLDINGS) = 3 THEN 'OK' ELSE 'Mismatch' END,
            		'BatchID values must match Audit table'
            UNION ALL    
            /* It is possible that the dimension record has changed between orgination of the trade and the completion of the trade. *
             * So, we can check that the Effective Date of the dimension record is older than the the completion date, but the end date could be earlier or later than the completion date
             */
            	SELECT
            		'FactHoldings SK_CustomerID',
            		NULL,
            --(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) 
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTHOLDINGS) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTHOLDINGS a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_DATEID JOIN TPCDI_WH.SILVER.DimCustomer c ON a.SK_CUSTOMERID = c.SK_CustomerID AND c.EffectiveDate <= _d.DateValue ) 
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_CustomerIDs match a DimCustomer record with a valid date range'
            UNION ALL
            	SELECT
            		'FactHoldings SK_AccountID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) 
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTHOLDINGS) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTHOLDINGS a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_DATEID JOIN TPCDI_WH.SILVER.DimAccount c ON a.SK_ACCOUNTID = c.SK_AccountID AND c.EffectiveDate <= _d.DateValue ) 
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_AccountIDs match a DimAccount record with a valid date range'
            		--good till here
            UNION ALL    
            	SELECT
            		'FactHoldings SK_CompanyID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) 
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTHOLDINGS) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTHOLDINGS a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_DATEID JOIN TPCDI_WH.SILVER.DimCompany c ON a.SK_COMPANYID = c.SK_CompanyID AND c.EffectiveDate <= _d.DateValue ) 
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_CompanyIDs match a DimCompany record with a valid date range'
            UNION ALL    
            	SELECT
            		'FactHoldings SK_SecurityID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) 
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTHOLDINGS) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTHOLDINGS a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_DATEID JOIN TPCDI_WH.SILVER.DimSecurity c ON a.SK_SECURITYID = c.SK_SecurityID AND c.EffectiveDate <= _d.DateValue ) 
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_SecurityIDs match a DimSecurity record with a valid date range'
            UNION ALL
            	SELECT
            		'FactHoldings CurrentTradeID',
            		NULL,
            		CASE WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTHOLDINGS) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FACTHOLDINGS a JOIN TPCDI_WH.SILVER.DimTrade t ON a.CurrentTradeID = t.TradeID AND a.SK_DATEID = t.SK_CloseDateID AND a.SK_TIMEID = t.SK_CloseTimeID) THEN 'OK' ELSE 'Failed' END,
            		'CurrentTradeID matches a DimTrade record with and Close Date and Time are values are used as the holdings date and time'
            UNION ALL
               SELECT
    'FactHoldings SK_DateID' ,
    a.BatchID,
    CASE  WHEN d1.MinDateValue < (SELECT MIN(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND BatchID = a.BatchID AND ATTRIBUTE = 'FirstDay')
    OR d2.MaxDateValue > (SELECT MAX(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND BatchID = a.BatchID AND ATTRIBUTE = 'LastDay') THEN 'Bad Date'
        ELSE 'OK'    END AS RESULT,
    'All dates are within batch date range' 
FROM
    (SELECT DISTINCT BatchID FROM TPCDI_WH.BRONZE.Audit WHERE BatchID IN (1, 2, 3)) a
JOIN (
    SELECT         m.BATCHID,        MIN(d.DateValue) AS MinDateValue,        MAX(d.DateValue) AS MaxDateValue
    FROM         TPCDI_WH.GOLD.FACTHOLDINGS m
    JOIN         TPCDI_WH.BRONZE.DimDate d ON m.SK_DATEID = d.SK_DateID
    GROUP BY         m.BATCHID
) AS d1 ON  d1.BATCHID =a.BatchID 
JOIN (
    SELECT         m.BATCHID,        MAX(d.DateValue) AS MaxDateValue
    FROM         TPCDI_WH.GOLD.FACTHOLDINGS m
    JOIN         TPCDI_WH.BRONZE.DimDate d ON m.SK_DATEID = d.SK_DateID
    GROUP BY         m.BATCHID
) AS d2 ON a.BatchID = d2.BATCHID
UNION ALL
            	SELECT
            		'FactCashBalances batches',
            		NULL,
            		CASE WHEN ( SELECT count(DISTINCT BatchID) FROM TPCDI_WH.GOLD.FactCashBalances) = 3 AND ( SELECT max(BatchID) FROM TPCDI_WH.GOLD.FactCashBalances) = 3 THEN 'OK' ELSE 'Mismatch' END,
            		'BatchID values must match Audit table'
            UNION ALL    
            	SELECT
            		'FactCashBalances SK_CustomerID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FactCashBalances) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FactCashBalances a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_DateID JOIN TPCDI_WH.SILVER.DimCustomer c ON a.SK_CustomerID = c.SK_CustomerID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate) 
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_CustomerIDs match a DimCustomer record with a valid date range'
            UNION ALL    
            	SELECT
            		'FactCashBalances SK_AccountID',
            		NULL,
            		--(select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)		
            		CASE 	WHEN ( SELECT count(*) FROM TPCDI_WH.GOLD.FactCashBalances) = ( SELECT count(*) FROM TPCDI_WH.GOLD.FactCashBalances a JOIN TPCDI_WH.BRONZE.DimDate _d ON _d.SK_DateID = a.SK_DateID JOIN TPCDI_WH.SILVER.DimAccount c ON a.SK_AccountID = c.SK_AccountID AND c.EffectiveDate <= _d.DateValue AND _d.DateValue <= c.EndDate) 
            				THEN 'OK' ELSE 'Bad join' END,
            		'All SK_AccountIDs match a DimAccount record with a valid date range'
            UNION ALL
            	-- Modified because of 1) too far nested correlated subqueries and 2) ANY correlated subqueries need aggregation (added first())
            	-- select 'FactCashBalances SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
            	--    select distinct BatchID, (
            	--          case when (
            	--                (select count(*) FROM TPCDI_WH.GOLD.FactCashBalances m where BatchID = a.BatchID and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = m.SK_DateID) < (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'FirstDay')) + 
            	--                (select count(*) FROM TPCDI_WH.GOLD.FactCashBalances m where BatchID = a.BatchID and (select DateValue FROM TPCDI_WH.BRONZE.DimDate where SK_DateID = m.SK_DateID) > (select Date FROM TPCDI_WH.BRONZE.Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'LastDay'))
            	--          ) = 0
            	--          then 'OK' else 'Bad Date' end
            	--    ) as Result
            	--    FROM TPCDI_WH.BRONZE.Audit a where BatchID in (1, 2, 3)
            	-- ) o    
            	SELECT
            		'FactCashBalances SK_DateID',
            		BatchID,
            		RESULT,
            		'All dates are within batch date range'
            	FROM	(	SELECT DISTINCT BatchID, 
            						( CASE	WHEN (	(	SELECT count(*) FROM TPCDI_WH.GOLD.FactCashBalances m 
            												WHERE BatchID = a.BatchID AND ( SELECT min(DateValue) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = m.SK_DateID) < ( SELECT min(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND BatchID = m.BatchID AND ATTRIBUTE = 'FirstDay')) + 
            										(	SELECT count(*) FROM TPCDI_WH.GOLD.FactCashBalances m 
            												WHERE BatchID = a.BatchID AND ( SELECT min(DateValue) FROM TPCDI_WH.BRONZE.DimDate WHERE SK_DateID = m.SK_DateID) > ( SELECT min(Date) FROM TPCDI_WH.BRONZE.Audit WHERE DataSet = 'Batch' AND BatchID = m.BatchID AND ATTRIBUTE = 'LastDay'))
            									 ) = 0 THEN 'OK'
            								ELSE 'Bad Date'	END
            						) AS RESULT 
            				FROM TPCDI_WH.BRONZE.Audit a WHERE BatchID IN (1, 2, 3)	) o
            /*
             *  Checks against the Batch Validation Query row counts
             */
            UNION ALL
            	SELECT
            		'Batch row count: ' || MessageSource,
            		BatchID,
            		CASE WHEN RowsLastBatch > RowsThisBatch THEN 'Row count decreased' ELSE 'OK' END,
            		'Row counts do not decrease between successive batches'
            	FROM	(	SELECT	DISTINCT(a.BatchID),
            						m.MessageSource,
            						CAST(m1.MessageData AS bigint) AS RowsThisBatch,
            						CAST(m2.MessageData AS bigint) AS RowsLastBatch
            				FROM TPCDI_WH.BRONZE.Audit a
            				FULL JOIN TPCDI_WH.SILVER.DIMessages m 	ON m.BatchID = 0 AND m.MessageText = 'Row count' AND m.MessageType = 'Validation'
            				JOIN TPCDI_WH.SILVER.DIMessages m1 ON m1.BatchID = a.BatchID AND m1.MessageSource = m.MessageSource AND m1.MessageText = 'Row count' AND m1.MessageType = 'Validation'
            				JOIN TPCDI_WH.SILVER.DIMessages m2 ON m2.BatchID = a.BatchID-1 AND m2.MessageSource = m.MessageSource AND m2.MessageText = 'Row count' AND m2.MessageType = 'Validation'
            				WHERE	a.BatchID IN (1, 2, 3)	) o
            UNION ALL
            	SELECT
            		'Batch joined row count: ' || MessageSource,
            		BatchID,
            		CASE WHEN RowsJoined = RowsUnjoined THEN 'OK' ELSE 'No match' END,
            		'Row counts match when joined to dimensions'
            	FROM	(	SELECT	DISTINCT(a.BatchID),
            						m.MessageSource,
            						CAST(m1.MessageData AS bigint) AS RowsUnjoined,
            						CAST(m2.MessageData AS bigint) AS RowsJoined
            				FROM TPCDI_WH.BRONZE.Audit a
            				FULL JOIN TPCDI_WH.SILVER.DIMessages m ON m.BatchID = 0 AND m.MessageText = 'Row count' AND m.MessageType = 'Validation'
            				JOIN TPCDI_WH.SILVER.DIMessages m1 ON m1.BatchID = a.BatchID AND m1.MessageSource = m.MessageSource AND m1.MessageText = 'Row count' AND m1.MessageType = 'Validation'
            				JOIN TPCDI_WH.SILVER.DIMessages m2 ON m2.BatchID = a.BatchID AND m2.MessageSource = m.MessageSource AND m2.MessageText = 'Row count joined' AND m2.MessageType = 'Validation'
            				WHERE	a.BatchID IN (1, 2, 3)	) o
            
            
            /*
             *  Checks against the Data Visibility Query row counts
             */
            UNION ALL
            	SELECT
            		'Data visibility row counts: ' || MessageSource ,
            		NULL AS BatchID,
            		CASE WHEN regressions = 0 THEN 'OK' ELSE 'Row count decreased' END,
            		'Row counts must be non-decreasing over time'
            	FROM	(	SELECT
            					m1.MessageSource,
            					sum( CASE WHEN CAST(m1.MessageData AS bigint) > CAST(m2.MessageData AS bigint) THEN 1 ELSE 0 END ) AS regressions
            				FROM TPCDI_WH.SILVER.DIMessages m1	JOIN TPCDI_WH.SILVER.DIMessages m2 
            						ON	m2.MessageType IN ('Visibility_1', 'Visibility_2') AND m2.MessageText = 'Row count' AND m2.MessageSource = m1.MessageSource AND m2.MessageDateAndTime > m1.MessageDateAndTime
            				WHERE	m1.MessageType IN ('Visibility_1', 'Visibility_2')	AND m1.MessageText = 'Row count'
            				GROUP BY	m1.MessageSource	) o
            UNION ALL
            	SELECT
            		'Data visibility joined row counts: ' || MessageSource ,
            		NULL AS BatchID,
            		CASE WHEN regressions = 0 THEN 'OK' ELSE 'No match' END,
            		'Row counts match when joined to dimensions'
            	FROM
            		(
            		SELECT
            			m1.MessageSource,
            			sum( CASE WHEN CAST(m1.MessageData AS bigint) > CAST(m2.MessageData AS bigint) THEN 1 ELSE 0 END) AS regressions
            		FROM TPCDI_WH.SILVER.DIMessages m1	JOIN TPCDI_WH.SILVER.DIMessages m2 
            				ON	m2.MessageType = 'Visibility_1'	AND	m2.MessageText = 'Row count joined'	AND	m2.MessageSource = m1.MessageSource	AND	m2.MessageDateAndTime = m1.MessageDateAndTime
            		WHERE	m1.MessageType = 'Visibility_1'	AND m1.MessageText = 'Row count'
            		GROUP BY	m1.MessageSource	) o
            
            /* close the outer query */
            ) q
            ORDER BY Test, Batch
        """).write.mode("append").save_as_table('TPCDI_WH.GOLD.automated_audit_results')
    
    
    for i in ['1','2','3']:    ##'1','2','3'
        batch = i

        ##-----------------------------------------------------------------------------------------------------------------------------
        ## creating schema & stage
        if batch == '1': 
            session.sql("CREATE OR REPLACE DATABASE TPCDI_WH").collect()
            session.sql("CREATE OR REPLACE DATABASE TPCDI_STG").collect()
            
            session.sql("CREATE OR REPLACE SCHEMA TPCDI_WH.BRONZE").collect()
            session.sql("CREATE OR REPLACE SCHEMA TPCDI_WH.SILVER").collect()
            session.sql("CREATE OR REPLACE SCHEMA TPCDI_WH.GOLD").collect()
            
            session.sql("CREATE OR REPLACE SCHEMA TPCDI_STG.REF").collect()
            session.sql("CREATE OR REPLACE SCHEMA TPCDI_STG.PUBLIC").collect()
            
            session.sql("CREATE OR REPLACE SCHEMA TPCDI_STG.BRONZE").collect()
            session.sql("CREATE OR REPLACE SCHEMA TPCDI_STG.SILVER").collect()
            session.sql(f"CREATE OR REPLACE STAGE TPCDI_STG.PUBLIC.{stage_name} URL = 's3://aws-tpcdi-test/Folder1/' DIRECTORY = ( ENABLE = true )").collect()
        ##-----------------------------------------------------------------------------------------------------------------------------
        
        
        ##-----------------------------------------------------------------------------------------------------------------------------   
        ## creating Sequences for Surrogate keys
        if batch == '1': 
            session.sql("CREATE OR REPLACE SEQUENCE TPCDI_STG.REF.DimBroker_SEQ START = 1 INCREMENT = 1").collect()
            session.sql("CREATE OR REPLACE SEQUENCE TPCDI_STG.REF.DimCompany_SEQ START = 1 INCREMENT = 1").collect()
            session.sql("CREATE OR REPLACE SEQUENCE TPCDI_STG.REF.DimSecurity_SEQ START = 1 INCREMENT = 1").collect()
            session.sql("CREATE OR REPLACE SEQUENCE TPCDI_STG.REF.DimCustomer_SEQ START = 1 INCREMENT = 1").collect()
            session.sql("CREATE OR REPLACE SEQUENCE TPCDI_STG.REF.DimAccount_SEQ START = 1 INCREMENT = 1").collect()
        
        ##-----------------------------------------------------------------------------------------------------------------------------   
        
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## Creating all the tables
        if batch == '1':    
            
            session.sql("create or replace table TPCDI_WH.BRONZE.Audit (dataset STRING NOT NULL COMMENT 'Component the data is associated with', batchid int COMMENT 'BatchID the data is associated with', date DATE COMMENT 'Date value corresponding to the Attribute', attribute STRING NOT NULL COMMENT 'Attribute this row of data corresponds to', value BIGINT COMMENT 'Integer value corresponding to the Attribute', dvalue FLOAT COMMENT 'Decimal value corresponding to the Attribute')").collect()

            session.sql("create or replace table TPCDI_WH.BRONZE.DimDate (sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date', datevalue DATE NOT NULL COMMENT 'The date stored appropriately for doing comparisons in the Data Warehouse', datedesc STRING NOT NULL COMMENT 'The date in full written form, e.g. July 7,2004', calendaryearid INT NOT NULL COMMENT 'Year number as a number', calendaryeardesc STRING NOT NULL COMMENT 'Year number as text', calendarqtrid INT NOT NULL COMMENT 'Quarter as a number, e.g. 20042', calendarqtrdesc STRING NOT NULL COMMENT 'Quarter as text, e.g. 2004 Q2', calendarmonthid INT NOT NULL COMMENT 'Month as a number, e.g. 20047', calendarmonthdesc STRING NOT NULL COMMENT 'Month as text, e.g. 2004 July', calendarweekid INT NOT NULL COMMENT 'Week as a number, e.g. 200428', calendarweekdesc STRING NOT NULL COMMENT 'Week as text, e.g. 2004-W28', dayofweeknum INT NOT NULL COMMENT 'Day of week as a number, e.g. 3', dayofweekdesc STRING NOT NULL COMMENT 'Day of week as text, e.g. Wednesday', fiscalyearid INT NOT NULL COMMENT 'Fiscal year as a number, e.g. 2005', fiscalyeardesc STRING NOT NULL COMMENT 'Fiscal year as text, e.g. 2005', fiscalqtrid INT NOT NULL COMMENT 'Fiscal quarter as a number, e.g. 20051', fiscalqtrdesc STRING NOT NULL COMMENT 'Fiscal quarter as text, e.g. 2005 Q1', holidayflag BOOLEAN COMMENT 'Indicates holidays')").collect()

            session.sql("create or replace table TPCDI_WH.BRONZE.Industry (in_id STRING NOT NULL COMMENT 'Industry code', in_name STRING NOT NULL COMMENT 'Industry description', in_sc_id STRING NOT NULL COMMENT 'Sector identifier')").collect()

            session.sql("create or replace table TPCDI_WH.BRONZE.StatusType (st_id STRING NOT NULL COMMENT 'Status code', st_name STRING NOT NULL COMMENT 'Status description')").collect()

            session.sql("create or replace table TPCDI_WH.BRONZE.TaxRate (tx_id STRING NOT NULL COMMENT 'Tax rate code', tx_name STRING NOT NULL COMMENT 'Tax rate description', tx_rate FLOAT NOT NULL COMMENT 'Tax rate')").collect()

            session.sql("create or replace table TPCDI_WH.BRONZE.TradeType (tt_id STRING NOT NULL COMMENT 'Trade type code', tt_name STRING NOT NULL COMMENT 'Trade type description', tt_is_sell INT NOT NULL COMMENT 'Flag indicating a sale', tt_is_mrkt INT NOT NULL COMMENT 'Flag indicating a market order')").collect()

            session.sql("create or replace table TPCDI_WH.BRONZE.DimTime (sk_timeid BIGINT NOT NULL COMMENT 'Surrogate key for the time', timevalue TIME NOT NULL COMMENT 'The time stored appropriately for doing', hourid INT NOT NULL COMMENT 'Hour number as a number, e.g. 01', hourdesc STRING NOT NULL COMMENT 'Hour number as text, e.g. 01', minuteid INT NOT NULL COMMENT 'Minute as a number, e.g. 23', minutedesc STRING NOT NULL COMMENT 'Minute as text, e.g. 01:23', secondid INT NOT NULL COMMENT 'Second as a number, e.g. 45', seconddesc STRING NOT NULL COMMENT 'Second as text, e.g. 01:23:45', markethoursflag BOOLEAN COMMENT 'Indicates a time during market hours', officehoursflag BOOLEAN COMMENT 'Indicates a time during office hours')").collect()

            session.sql("create or replace table TPCDI_WH.BRONZE.BatchDate (batchdate DATE NOT NULL COMMENT 'Batch date', batchid INT COMMENT 'Batch ID when this record was inserted')").collect()

            session.sql("create or replace table TPCDI_STG.BRONZE.FINWIRE (LINE STRING, REC_TYPE STRING, PTS TIMESTAMP)").collect()
            
            session.sql("create or replace table TPCDI_WH.SILVER.DIMessages (MessageDateAndTime TIMESTAMP NOT NULL COMMENT 'Date and time of the message', BatchId INT NOT NULL COMMENT 'DI run number; see the section Overview of BatchID usage', MessageSource STRING COMMENT 'Typically the name of the transform that logs the message', MessageText STRING NOT NULL COMMENT 'Description of why the message was logged', MessageType STRING NOT NULL COMMENT 'Status or Alert or Reject', MessageData STRING  COMMENT 'Varies with the reason for logging the message')").collect()
            
            session.sql("create or replace table TPCDI_WH.SILVER.DimBroker (sk_brokerid BIGINT NOT NULL comment 'Surrogate key for broker', brokerid BIGINT NOT NULL comment 'Natural key for broker', managerid BIGINT comment 'Natural key for manager’s HR record', firstname STRING NOT NULL comment 'First name', lastname STRING NOT NULL comment 'Last Name', middleinitial STRING comment 'Middle initial', branch STRING comment 'Facility in which employee has office', office STRING comment 'Office number or description', phone STRING comment 'Employee phone number', iscurrent BOOLEAN NOT NULL comment 'True if this is the current record', batchid INT NOT NULL comment 'Batch ID when this record was inserted', effectivedate DATE NOT NULL comment 'Beginning of date range when this record was the current record', enddate DATE NOT NULL comment 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.' )").collect()

            session.sql("create or replace table TPCDI_WH.SILVER.DimCompany (sk_companyid BIGINT NOT NULL COMMENT 'Surrogate key for CompanyID', companyid BIGINT NOT NULL COMMENT 'Company identifier (CIK number)', status STRING NOT NULL COMMENT 'Company status', name STRING NOT NULL COMMENT 'Company name', industry STRING NOT NULL COMMENT 'Company’s industry', sprating STRING COMMENT 'Standard & Poor company’s rating', islowgrade BOOLEAN COMMENT 'True if this company is low grade', ceo STRING NOT NULL COMMENT 'CEO name', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING NOT NULL COMMENT 'Zip or postal code', city STRING NOT NULL COMMENT 'City', stateprov STRING NOT NULL COMMENT 'State or Province', country STRING COMMENT 'Country', description STRING NOT NULL COMMENT 'Company description', foundingdate DATE NULL COMMENT 'Date the company was founded', iscurrent BOOLEAN NOT NULL COMMENT 'True if this is the current record', batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted', effectivedate DATE NOT NULL COMMENT 'Beginning of date range when this record was the current record', enddate DATE NOT NULL COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.')").collect()

            session.sql("create or replace table TPCDI_WH.SILVER.DimSecurity (sk_securityid BIGINT NOT NULL COMMENT 'Surrogate key for Symbol', symbol STRING NOT NULL COMMENT 'Identifies security on ticker', issue STRING NOT NULL COMMENT 'Issue type', status STRING NOT NULL COMMENT 'Status type', name STRING NOT NULL COMMENT 'Security name', exchangeid STRING NOT NULL COMMENT 'Exchange the security is traded on', sk_companyid BIGINT NOT NULL COMMENT 'Company issuing security', sharesoutstanding BIGINT NOT NULL COMMENT 'Shares outstanding', firsttrade DATE NOT NULL COMMENT 'Date of first trade', firsttradeonexchange DATE NOT NULL COMMENT 'Date of first trade on this exchange', dividend DOUBLE NOT NULL COMMENT 'Annual dividend per share', iscurrent BOOLEAN NOT NULL COMMENT 'True if this is the current record', batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted', effectivedate DATE NOT NULL COMMENT 'Beginning of date range when this record was the current record', enddate DATE NOT NULL COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.')").collect()

            session.sql("create or replace table TPCDI_WH.SILVER.DimAccount CLUSTER BY (iscurrent) (sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for AccountID', accountid BIGINT NOT NULL COMMENT 'Customer account identifier', sk_brokerid BIGINT NOT NULL COMMENT 'Surrogate key of managing broker', sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key of customer', accountdesc STRING COMMENT 'Name of customer account', taxstatus INT COMMENT 'Tax status of this account', status STRING NOT NULL COMMENT 'Account status, active or closed', iscurrent BOOLEAN NOT NULL COMMENT 'True if this is the current record', batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted', effectivedate DATE NOT NULL COMMENT 'Beginning of date range when this record was the current record', enddate DATE NOT NULL COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.')").collect()

            session.sql("create or replace table TPCDI_WH.SILVER.Financial (sk_companyid BIGINT NOT NULL COMMENT 'Company SK.', fi_year INT NOT NULL COMMENT 'Year of the quarter end.', fi_qtr INT NOT NULL COMMENT 'Quarter number that the financial information is for: valid values 1, 2, 3, 4.', fi_qtr_start_date DATE NOT NULL COMMENT 'Start date of quarter.', fi_revenue DOUBLE NOT NULL COMMENT 'Reported revenue for the quarter.', fi_net_earn DOUBLE NOT NULL COMMENT 'Net earnings reported for the quarter.', fi_basic_eps DOUBLE NOT NULL COMMENT 'Basic earnings per share for the quarter.', fi_dilut_eps DOUBLE NOT NULL COMMENT 'Diluted earnings per share for the quarter.', fi_margin DOUBLE NOT NULL COMMENT 'Profit divided by revenues for the quarter.', fi_inventory DOUBLE NOT NULL COMMENT 'Value of inventory on hand at the end of quarter.', fi_assets DOUBLE NOT NULL COMMENT 'Value of total assets at the end of the quarter.', fi_liability DOUBLE NOT NULL COMMENT 'Value of total liabilities at the end of the quarter.', fi_out_basic BIGINT NOT NULL COMMENT 'Average number of shares outstanding (basic).', fi_out_dilut BIGINT NOT NULL COMMENT 'Average number of shares outstanding (diluted).')").collect()
                
            session.sql("create or replace table TPCDI_WH.SILVER.PROSPECT (agencyid STRING NOT NULL COMMENT 'Unique identifier from agency', sk_recorddateid BIGINT NOT NULL  COMMENT 'Last date this prospect appeared in input', sk_updatedateid BIGINT NOT NULL COMMENT 'Latest change date for this prospect', batchid INT NOT NULL COMMENT 'Batch ID when this record was last modified', iscustomer BOOLEAN NOT NULL COMMENT 'True if this person is also in DimCustomer,else False', lastname STRING NOT NULL COMMENT 'Last name', firstname STRING NOT NULL COMMENT 'First name', middleinitial STRING COMMENT 'Middle initial', gender STRING COMMENT 'M / F / U', addressline1 STRING COMMENT 'Postal address', addressline2 STRING COMMENT 'Postal address', postalcode STRING COMMENT 'Postal code', city STRING NOT NULL COMMENT 'City', state STRING NOT NULL COMMENT 'State or province', country STRING COMMENT 'Postal country', phone STRING COMMENT 'Telephone number', income int COMMENT 'Annual income', numbercars INT COMMENT 'Cars owned', numberchildren INT COMMENT 'Dependent children', maritalstatus STRING COMMENT 'S / M / D / W / U', age INT COMMENT 'Current age', creditrating INT COMMENT 'Numeric rating', ownorrentflag STRING COMMENT 'O / R / U', employer STRING COMMENT 'Name of employer', numbercreditcards INT COMMENT 'Credit cards', networth INT COMMENT 'Estimated total net worth', marketingnameplate STRING COMMENT 'For marketing purposes')").collect()
            
            session.sql("create or replace table TPCDI_WH.SILVER.DimCustomer (sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for CustomerID', customerid BIGINT NOT NULL COMMENT 'Customer identifier', taxid STRING NOT NULL COMMENT 'Customer’s tax identifier', status STRING NOT NULL COMMENT 'Customer status type', lastname STRING NOT NULL COMMENT 'Customers last name.', firstname STRING NOT NULL COMMENT 'Customers first name.', middleinitial STRING COMMENT 'Customers middle name initial', gender STRING COMMENT 'Gender of the customer', tier TINYINT COMMENT 'Customer tier', dob DATE NOT NULL COMMENT 'Customer’s date of birth.', addressline1 STRING NOT NULL COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING NOT NULL COMMENT 'Zip or Postal Code', city STRING NOT NULL COMMENT 'City', stateprov STRING NOT NULL COMMENT 'State or Province', country STRING COMMENT 'Country', phone1 STRING COMMENT 'Phone number 1', phone2 STRING COMMENT 'Phone number 2', phone3 STRING COMMENT 'Phone number 3', email1 STRING COMMENT 'Email address 1', email2 STRING COMMENT 'Email address 2', nationaltaxratedesc STRING COMMENT 'National Tax rate description', nationaltaxrate FLOAT COMMENT 'National Tax rate', localtaxratedesc STRING COMMENT 'Local Tax rate description', localtaxrate FLOAT COMMENT 'Local Tax rate', agencyid STRING COMMENT 'Agency identifier', creditrating INT COMMENT 'Credit rating', networth INT COMMENT 'Net worth', marketingnameplate STRING COMMENT 'Marketing nameplate', iscurrent BOOLEAN NOT NULL COMMENT 'True if this is the current record', batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted', effectivedate DATE NOT NULL COMMENT 'Beginning of date range when this record was the current record', enddate DATE NOT NULL COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.')").collect()

            session.sql("create or replace table TPCDI_WH.GOLD.FactMarketHistory CLUSTER BY (batchid) (sk_securityid BIGINT NOT NULL COMMENT 'Surrogate key for SecurityID', sk_companyid BIGINT NOT NULL COMMENT 'Surrogate key for CompanyID', sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date', peratio DOUBLE COMMENT 'Price to earnings per share ratio', yield DOUBLE NOT NULL COMMENT 'Dividend to price ratio, as a percentage', fiftytwoweekhigh DOUBLE NOT NULL COMMENT 'Security highest price in last 52 weeks from this day', sk_fiftytwoweekhighdate BIGINT NOT NULL COMMENT 'Earliest date on which the 52 week high price was set', fiftytwoweeklow DOUBLE NOT NULL COMMENT 'Security lowest price in last 52 weeks from this day', sk_fiftytwoweeklowdate BIGINT NOT NULL COMMENT 'Earliest date on which the 52 week low price was set', closeprice DOUBLE NOT NULL COMMENT 'Security closing price on this day', dayhigh DOUBLE NOT NULL COMMENT 'Highest price for the security on this day', daylow DOUBLE NOT NULL COMMENT 'Lowest price for the security on this day', volume INT NOT NULL COMMENT 'Trading volume of the security on this day', batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted')").collect()

            session.sql("create or replace table TPCDI_WH.GOLD.FactCashBalances (sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for CustomerID', sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for AccountID', sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date', cash DOUBLE NOT NULL COMMENT 'Cash balance for the account after applying', batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted')").collect()
            
            session.sql("create or replace table TPCDI_WH.SILVER.DimTrade (tradeid BIGINT NOT NULL COMMENT 'Trade identifier', sk_brokerid BIGINT COMMENT 'Surrogate key for BrokerID', sk_createdateid BIGINT NOT NULL COMMENT 'Surrogate key for date created', sk_createtimeid BIGINT NOT NULL COMMENT 'Surrogate key for time created', sk_closedateid BIGINT COMMENT 'Surrogate key for date closed', sk_closetimeid BIGINT COMMENT 'Surrogate key for time closed', status STRING NOT NULL COMMENT 'Trade status', type STRING NOT NULL COMMENT 'Trade type', cashflag BOOLEAN NOT NULL COMMENT 'Is this trade a cash (1) or margin (0) trade?', sk_securityid BIGINT NOT NULL COMMENT 'Surrogate key for SecurityID', sk_companyid BIGINT NOT NULL COMMENT 'Surrogate key for CompanyID', quantity INT NOT NULL COMMENT 'Quantity of securities traded.', bidprice DOUBLE NOT NULL COMMENT 'The requested unit price.', sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for CustomerID', sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for AccountID', executedby STRING NOT NULL COMMENT 'Name of person executing the trade.', tradeprice DOUBLE COMMENT 'Unit price at which the security was traded.', fee DOUBLE COMMENT 'Fee charged for placing this trade request', commission DOUBLE COMMENT 'Commission earned on this trade', tax DOUBLE COMMENT 'Amount of tax due on this trade', batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted')").collect()
                
            session.sql("create OR replace table TPCDI_WH.GOLD.FACTWATCHES (SK_CustomerID BIGINT NOT NULL COMMENT 'Customer associated with watch list',SK_SecurityID BIGINT NOT NULL COMMENT 'Security listed on watch list',SK_DateID_DatePlaced VARCHAR(8) NOT NULL COMMENT 'Date the watch list item was added',SK_DateID_DateRemoved VARCHAR(8) COMMENT 'Date the watch list item was removed',BatchID INT NOT NULL COMMENT 'Batch ID when this record was inserted')").collect()

            session.sql("CREATE OR REPLACE TABLE TPCDI_WH.GOLD.FACTHOLDINGS (TradeID INT NOT NULL COMMENT 'Key for Orignial Trade Indentifier',CurrentTradeID INT NOT NULL COMMENT 'Key for the current trade',SK_CUSTOMERID BIGINT NOT NULL COMMENT 'Surrogate key for Customer Identifier',SK_ACCOUNTID BIGINT NOT NULL COMMENT 'Surrogate key for Account Identifier',SK_SECURITYID BIGINT NOT NULL COMMENT 'Surrogate key for Security Identifier',SK_COMPANYID BIGINT NOT NULL COMMENT 'Surrogate key for Company Identifier',SK_DATEID BIGINT NOT NULL COMMENT 'Surrogate key for the date associated with the current trade',SK_TIMEID BIGINT NOT NULL COMMENT 'Surrogate key for the time associated with the current trade',CURRENTPRICE DOUBLE COMMENT 'Unit price of this security for the current trade',CURRENTHOLDING INT NOT NULL COMMENT 'Quantity of a security held after the current trade.The value can be a positive or negative integer',BATCHID INT NOT NULL COMMENT 'Batch ID when this record was inserted')").collect()

            session.sql("CREATE OR REPLACE TABLE TPCDI_WH.GOLD.automated_audit_results (Test STRING,Batch INT, Result STRING ,Description STRING)").collect()
            
            session.sql("create or replace table TPCDI_STG.SILVER.DimCustomerStg CLUSTER BY (iscurrent) (sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID', customerid BIGINT COMMENT 'Customer identifier', taxid STRING COMMENT 'Customer’s tax identifier', status STRING COMMENT 'Customer status type', lastname STRING COMMENT 'Customers last name.', firstname STRING COMMENT 'Customers first name.', middleinitial STRING COMMENT 'Customers middle name initial', gender STRING COMMENT 'Gender of the customer', tier TINYINT COMMENT 'Customer tier', dob DATE COMMENT 'Customer’s date of birth.', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or Postal Code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or Province', country STRING COMMENT 'Country', phone1 STRING COMMENT 'Phone number 1', phone2 STRING COMMENT 'Phone number 2', phone3 STRING COMMENT 'Phone number 3', email1 STRING COMMENT 'Email address 1', email2 STRING COMMENT 'Email address 2', lcl_tx_id STRING COMMENT 'Customers local tax rate', nat_tx_id STRING COMMENT 'Customers national tax rate', batchid INT COMMENT 'Batch ID when this record was inserted', iscurrent BOOLEAN COMMENT 'True if this is the current record', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.')").collect()

            session.sql("create or replace table TPCDI_STG.BRONZE.CustomerIncremental CLUSTER BY (batchid) (cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', customerid BIGINT COMMENT 'Customer identifier', taxid STRING COMMENT 'Customer’s tax identifier', status STRING COMMENT 'Customer status type identifier', lastname STRING COMMENT 'Primary Customers last name.', firstname STRING COMMENT 'Primary Customers first name.', middleinitial STRING COMMENT 'Primary Customers middle initial', gender STRING COMMENT 'Gender of the primary customer', tier TINYINT COMMENT 'Customer tier', dob DATE COMMENT 'Customer’s date of birth, as YYYY-MM-DD.', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or postal code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or province', country STRING COMMENT 'Country', c_ctry_1 STRING COMMENT 'Country code for Customers phone 1.', c_area_1 STRING COMMENT 'Area code for customer’s phone 1.', c_local_1 STRING COMMENT 'Local number for customer’s phone 1.', c_ext_1 STRING COMMENT 'Extension number for Customer’s phone 1.', c_ctry_2 STRING COMMENT 'Country code for Customers phone 2.', c_area_2 STRING COMMENT 'Area code for Customer’s phone 2.', c_local_2 STRING COMMENT 'Local number for Customer’s phone 2.', c_ext_2 STRING COMMENT 'Extension number for Customer’s phone 2.', c_ctry_3 STRING COMMENT 'Country code for Customers phone 3.', c_area_3 STRING COMMENT 'Area code for Customer’s phone 3.', c_local_3 STRING COMMENT 'Local number for Customer’s phone 3.', c_ext_3 STRING COMMENT 'Extension number for Customer’s phone 3.', email1 STRING COMMENT 'Customers e-mail address 1.', email2 STRING COMMENT 'Customers e-mail address 2.', lcl_tx_id STRING COMMENT 'Customers local tax rate', nat_tx_id STRING COMMENT 'Customers national tax rate', batchid INT COMMENT 'Batch ID when this record was inserted')").collect()
        
            session.sql("create or replace table TPCDI_STG.BRONZE.AccountIncremental (cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', accountid BIGINT COMMENT 'Customer account identifier', ca_b_id BIGINT COMMENT 'Identifier of the managing broker', ca_c_id BIGINT COMMENT 'Owning customer identifier', accountDesc STRING COMMENT 'Name of customer account', TaxStatus TINYINT COMMENT 'Tax status of this account', ca_st_id STRING COMMENT 'Customer status type identifier', batchid INT COMMENT 'Batch ID when this record was inserted')").collect()

            session.sql("create or replace table TPCDI_STG.BRONZE.ProspectIncremental (agencyid STRING COMMENT 'Unique identifier from agency', lastname STRING COMMENT 'Last name', firstname STRING COMMENT 'First name', middleinitial STRING COMMENT 'Middle initial', gender STRING COMMENT '‘M’ or ‘F’ or ‘U’', addressline1 STRING COMMENT 'Postal address', addressline2 STRING COMMENT 'Postal address', postalcode STRING COMMENT 'Postal code', city STRING COMMENT 'City', state STRING COMMENT 'State or province', country STRING COMMENT 'Postal country', phone STRING COMMENT 'Telephone number', income STRING COMMENT 'Annual income', numbercars INT COMMENT 'Cars owned', numberchildren INT COMMENT 'Dependent children', maritalstatus STRING COMMENT '‘S’ or ‘M’ or ‘D’ or ‘W’ or ‘U’', age INT COMMENT 'Current age', creditrating INT COMMENT 'Numeric rating', ownorrentflag STRING COMMENT '‘O’ or ‘R’ or ‘U’', employer STRING COMMENT 'Name of employer', numbercreditcards INT COMMENT 'Credit cards', networth INT COMMENT 'Estimated total net worth', batchid INT COMMENT 'Batch ID when this record was inserted')").collect()

            session.sql("create or replace table TPCDI_STG.BRONZE.HoldingIncremental (cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', hh_h_t_id INT COMMENT 'Trade Identifier of the trade that originally created the holding row.', hh_t_id INT COMMENT 'Trade Identifier of the current trade', hh_before_qty INT COMMENT 'Quantity of this security held before the modifying trade.', hh_after_qty INT COMMENT 'Quantity of this security held after the modifying trade.', batchid INT COMMENT 'Batch ID when this record was inserted')").collect()

            session.sql("create or replace table TPCDI_STG.BRONZE.WatchIncremental (cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', w_c_id BIGINT COMMENT 'Customer identifier', w_s_symb STRING COMMENT 'Symbol of the security to watch', w_dts TIMESTAMP COMMENT 'Date and Time Stamp for the action', w_action STRING COMMENT 'Whether activating or canceling the watch', batchid INT COMMENT 'Batch ID when this record was inserted')").collect()

            session.sql("create or replace table TPCDI_STG.BRONZE.DailyMarketIncremental (cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', dm_date DATE COMMENT 'Date of last completed trading day.', dm_s_symb STRING COMMENT 'Security symbol of the security', dm_close DOUBLE COMMENT 'Closing price of the security on this day.', dm_high DOUBLE COMMENT 'Highest price for the security on this day.', dm_low DOUBLE COMMENT 'Lowest price for the security on this day.', dm_vol INT COMMENT 'Volume of the security on this day.', batchid INT comment 'Batch ID when this record was inserted')").collect()
                
            session.sql("create or replace table TPCDI_STG.BRONZE.CashTransactionIncremental (cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', ct_ca_id BIGINT COMMENT 'Customer account identifier', ct_dts TIMESTAMP COMMENT 'Timestamp of when the trade took place', ct_amt DOUBLE COMMENT 'Amount of the cash transaction.', ct_name STRING COMMENT 'Transaction name, or description: e.g. Cash from sale of DuPont stock.', batchid INT comment 'Batch ID when this record was inserted')").collect()

            session.sql("create or replace table TPCDI_STG.BRONZE.TradeIncremental (cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', t_id BIGINT COMMENT 'Trade identifier.', t_dts TIMESTAMP COMMENT 'Date and time of trade.', t_st_id STRING COMMENT 'Status type identifier', t_tt_id STRING COMMENT 'Trade type identifier', t_is_cash TINYINT COMMENT 'Is this trade a cash (‘1’) or margin (‘0’) trade?', t_s_symb STRING COMMENT 'Security symbol of the security', t_qty INT COMMENT 'Quantity of securities traded.', t_bid_price DOUBLE COMMENT 'The requested unit price.', t_ca_id BIGINT COMMENT 'Customer account identifier.', t_exec_name STRING COMMENT 'Name of the person executing the trade.', t_trade_price DOUBLE COMMENT 'Unit price at which the security was traded.', t_chrg DOUBLE COMMENT 'Fee charged for placing this trade request.', t_comm DOUBLE COMMENT 'Commission earned on this trade', t_tax DOUBLE COMMENT 'Amount of tax due on this trade', batchid INT comment 'Batch ID when this record was inserted')").collect()
         
        else:
            session.sql("delete from TPCDI_STG.BRONZE.CustomerIncremental").collect()
        
            session.sql("delete from TPCDI_STG.BRONZE.AccountIncremental").collect()
            
            session.sql("delete from TPCDI_STG.BRONZE.HoldingIncremental").collect()
            
            session.sql("delete from TPCDI_STG.BRONZE.WatchIncremental").collect()
            
            session.sql("delete from TPCDI_STG.BRONZE.DailyMarketIncremental").collect()
            
            session.sql("delete from TPCDI_STG.BRONZE.TradeIncremental").collect()
            
    
        ##-----------------------------------------------------------------------------------------------------------------------------
    
        
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## Batch0 Validation script
        if batch == '1':  
            batch_validation(0)     
        ##-----------------------------------------------------------------------------------------------------------------------------
    
    
        
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## For file0A: All Audit file
        con_file_name = '*_audit.csv'
        if file_name in ['all', con_file_name]:
            schema = StructType([
                    StructField("dataset", StringType(), False),
                    StructField("batchid", IntegerType(), False),
                    StructField("date", DateType(), False),
                    StructField("attribute", StringType(), False),
                    StructField("value", IntegerType(), False),
                    StructField("dvalue", FloatType(), False)
            ])
            load_csv_table(schema, con_file_name,'TPCDI_WH.BRONZE.Audit', ',')
        ##-----------------------------------------------------------------------------------------------------------------------------
    
        
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## For file0B: BatchDate.txt
        con_file_name = 'BatchDate.txt'
        if file_name in ['all', con_file_name]:
            schema = StructType([
                    StructField("BatchDate", DateType(), False)
            ])        
            load_csv_table(schema, con_file_name, 'TPCDI_WH.BRONZE.BatchDate', ',')
        ##-----------------------------------------------------------------------------------------------------------------------------
    
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## files which load only from Batch1
            
        if batch == '1':  
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file1: date.txt
            con_file_name = 'Date.txt'
            if file_name in ['all', con_file_name]:
                schema = StructType([
                        StructField("sk_dateid", IntegerType(), False),
                        StructField("datevalue", DateType(), False),
                        StructField("datedesc", StringType(), False),
                        StructField("calendaryearid", IntegerType(), False),
                        StructField("calendaryeardesc", StringType(), False),
                        StructField("calendarqtrid", IntegerType(), False),
                        StructField("calendarqtrdesc", StringType(), False),
                        StructField("calendarmonthid", IntegerType(), False),
                        StructField("calendarmonthdesc", StringType(), False),
                        StructField("calendarweekid", IntegerType(), False),
                        StructField("calendarweekdesc", StringType(), False),
                        StructField("dayofweeknum", IntegerType(), False),
                        StructField("dayofweekdesc", StringType(), False),
                        StructField("fiscalyearid", IntegerType(), False),
                        StructField("fiscalyeardesc", StringType(), False),
                        StructField("fiscalqtrid", IntegerType(), False),
                        StructField("fiscalqtrdesc", StringType(), False),
                        StructField("holidayflag", BooleanType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_WH.BRONZE.DimDate', '|')
            ##-----------------------------------------------------------------------------------------------------------------------------
            
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file2: hr.csv
            con_file_name = 'HR.csv'
            if file_name in ['all', con_file_name]:
                schema = StructType([
                        StructField("employeeid", IntegerType(), False),
                        StructField("managerid", IntegerType(), False),
                        StructField("employeefirstname", StringType(), True),
                        StructField("employeelastname", StringType(), True),
                        StructField("employeemi", StringType(), True),
                        StructField("employeejobcode", IntegerType(), True),
                        StructField("employeebranch", StringType(), True),
                        StructField("employeeoffice", StringType(), True),
                        StructField("employeephone", StringType(), True)
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.HR', ',')
            ##-----------------------------------------------------------------------------------------------------------------------------
            
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file3: Industry.txt
            con_file_name = 'Industry.txt'
            if file_name in ['all', con_file_name]:
                schema = StructType([
                        StructField("in_id", StringType(), False),
                        StructField("in_name", StringType(), False),
                        StructField("in_sc_id", StringType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_WH.BRONZE.Industry', '|')
            ##-----------------------------------------------------------------------------------------------------------------------------
        
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file4: TaxRate.txt
            con_file_name = 'TaxRate.txt'
            if file_name in ['all', con_file_name]:
                schema = StructType([
                        StructField("tx_id", StringType(), False),
                        StructField("tx_name", StringType(), True),
                        StructField("tx_rate", FloatType(), True),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_WH.BRONZE.TaxRate', '|')
            ##-----------------------------------------------------------------------------------------------------------------------------
        
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file5: StatusType.txt
            con_file_name = 'StatusType.txt'
            if file_name in ['all', con_file_name]:
                schema = StructType([
                        StructField("st_id", StringType(), False),
                        StructField("st_name", StringType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_WH.BRONZE.StatusType', '|')
            ##-----------------------------------------------------------------------------------------------------------------------------
        
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file6: TradeType.txt
            con_file_name = 'TradeType.txt'
            if file_name in ['all', con_file_name]:
                schema = StructType([
                        StructField("tt_id", StringType(), False),
                        StructField("tt_name", StringType(), False),
                        StructField("tt_is_sell", IntegerType(), False),
                        StructField("tt_is_mrkt", IntegerType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_WH.BRONZE.TradeType', '|')
            ##-----------------------------------------------------------------------------------------------------------------------------
        
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file7: Time.txt
            con_file_name = 'Time.txt'
            if file_name in ['all', con_file_name]:
                schema = StructType([
                        StructField("sk_timeid", IntegerType(), False),
                        StructField("timevalue", StringType(), False),
                        StructField("hourid", IntegerType(), False),
                        StructField("hourdesc", StringType(), False),
                        StructField("minuteid", IntegerType(), False),
                        StructField("minutedesc", StringType(), False),                
                        StructField("secondid", IntegerType(), False),
                        StructField("seconddesc", StringType(), False),
                        StructField("markethoursflag", BooleanType(), False),
                        StructField("officehoursflag", BooleanType(), False)
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_WH.BRONZE.DimTime', '|')
            ##-----------------------------------------------------------------------------------------------------------------------------
        
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file8: FINWIRE
            con_file_name = 'FINWIRE'
            if file_name in ['all', con_file_name]:
                # These are fixed-width fields, so read the entire line in as "line"
                schema = StructType([
                        StructField("line", StringType(), False),
                ])
        
                stage_path = get_stage_path(con_file_name)
                
        
                # generic dataframe for all record types
                # create a temporary table
                session \
                    .read \
                    .schema(schema) \
                    .option('field_delimiter', '|') \
                    .csv(stage_path) \
                    .with_column('rec_type', substring(col("line"), lit(16), lit(3))) \
                    .with_column('pts', to_timestamp(substring(col("line"), lit(0), lit(15)), lit("yyyymmdd-hhmiss"))) \
                    .write.mode("overwrite").save_as_table("TPCDI_STG.BRONZE.finwire") #, table_type="temporary")
        
                # CMP record types
                session \
                .table('TPCDI_STG.BRONZE.finwire') \
                .where(col('rec_type') == 'CMP') \
                .with_column('companyname', substr(col('line'), lit(19), lit(60))) \
                .withColumn('cik', substring(col("line"), lit(79), lit(10))) \
                .withColumn('status', substring(col("line"), lit(89), lit(4))) \
                .withColumn('industryid', substring(col("line"), lit(93), lit(2))) \
                .withColumn('SPrating', substring(col("line"), lit(95), lit(4))) \
                .withColumn('foundingdate', substring(col("line"), lit(99), lit(8))) \
                .withColumn('addrline1', substring(col("line"), lit(107), lit(80))) \
                .withColumn('addrline2', substring(col("line"), lit(187), lit(80))) \
                .withColumn('postalcode', substring(col("line"), lit(267), lit(12))) \
                .withColumn('city', substring(col("line"), lit(279), lit(25))) \
                .withColumn('stateprovince', substring(col("line"), lit(304), lit(20))) \
                .withColumn('country', substring(col("line"), lit(324), lit(24))) \
                .withColumn('ceoname', substring(col("line"), lit(348), lit(46))) \
                .withColumn('description', substring(col("line"), lit(394), lit(150))) \
                .drop(col('line')) \
                .createOrReplaceTempView("TPCDI_STG.BRONZE.FinwireCmpView")
        
                # SEC record types
                session \
                .table('TPCDI_STG.BRONZE.finwire') \
                .where(col('rec_type') == 'SEC') \
                .withColumn('effectivedate', to_date(substring(col("line"), lit(1), lit(8)), 'YYYYMMDD' )) \
                .withColumn('Symbol', substring(col("line"), lit(19), lit(15))) \
                .withColumn('issue', substring(col("line"), lit(34), lit(6))) \
                .withColumn('Status', substring(col("line"), lit(40), lit(4))) \
                .withColumn('Name', substring(col("line"), lit(44), lit(70))) \
                .withColumn('exchangeid', substring(col("line"), lit(114), lit(6))) \
                .withColumn('sharesoutstanding', substring(col("line"), lit(120), lit(13))) \
                .withColumn('firsttrade', substring(col("line"), lit(133), lit(8))) \
                .withColumn('firsttradeonexchange', substring(col("line"), lit(141), lit(8))) \
                .withColumn('Dividend', substring(col("line"), lit(149), lit(12))) \
                .withColumn('conameorcik', substring(col("line"), lit(161), lit(60))) \
                .drop(col('line')) \
                .createOrReplaceTempView("TPCDI_STG.BRONZE.FinwireSecView")
                
        
                # FIN record types
                session \
                .table('TPCDI_STG.BRONZE.finwire') \
                .where(col('rec_type') == 'FIN') \
                .withColumn('PTS', to_date(substring(col("line"), lit(1), lit(8)), 'YYYYMMDD')) \
                .withColumn('fi_year', substring(col("line"), lit(19), lit(4))) \
                .withColumn('fi_qtr', substring(col("line"), lit(23), lit(1))) \
                .withColumn('fi_qtr_start_date', to_date(substring(col("line"), lit(24), lit(8)), 'yyyyMMdd')) \
                .withColumn('fi_revenue', substring(col("line"), lit(40), lit(17))) \
                .withColumn('fi_net_earn', substring(col("line"), lit(57), lit(17))) \
                .withColumn('fi_basic_eps', substring(col("line"), lit(74), lit(12))) \
                .withColumn('fi_dilut_eps', substring(col("line"), lit(86), lit(12))) \
                .withColumn('fi_margin', substring(col("line"), lit(98), lit(12))) \
                .withColumn('fi_inventory', substring(col("line"), lit(110), lit(17))) \
                .withColumn('fi_assets', substring(col("line"), lit(127), lit(17))) \
                .withColumn('fi_liability', substring(col("line"), lit(144), lit(17))) \
                .withColumn('fi_out_basic', substring(col("line"), lit(161), lit(13))) \
                .withColumn('fi_out_dilut', substring(col("line"), lit(174), lit(13))) \
                .withColumn('conameorcik', substring(col("line"), lit(187), lit(60))) \
                .drop(col("line")) \
                .createOrReplaceTempView("TPCDI_STG.BRONZE.FinwireFinView")
            ##-----------------------------------------------------------------------------------------------------------------------------
        
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file9: CustomerMgmt.xml
            con_file_name = 'CustomerMgmt.xml'
            if file_name in ['all', con_file_name]:
        
                # upload_files(con_file_name, get_stage_path(stage, con_file_name))
        
                # this might get hairy
                df = session.read \
                    .option('STRIP_OUTER_ELEMENT', True) \
                    .xml(get_stage_path(con_file_name)) \
                    .select(
                        col('$1'),
                        xmlget(col('$1'), lit('Customer'), 0).alias('customer'),
                        xmlget(col('customer'), lit('Name'), 0).alias('name'),
                        xmlget(col('customer'), lit('Address'), 0).alias('address'),
                        xmlget(col('customer'), lit('ContactInfo'), 0).alias('contact_info'),
                        xmlget(col('contact_info'), lit('C_PHONE_1')).alias('phone1'),
                        xmlget(col('contact_info'), lit('C_PHONE_2')).alias('phone2'),
                        xmlget(col('contact_info'), lit('C_PHONE_3')).alias('phone3'),
                        xmlget(col('customer'), lit('TaxInfo'), 0).alias('tax_info'),
                        xmlget(col('customer'), lit('Account'), 0).alias('account'),
                    ) \
                    .select(
                        col('*'),
                        get(col('$1'), lit('@ActionTS')).cast('STRING').alias('action_ts'),
                        get_xml_element('phone1', 'C_CTRY_CODE', 'STRING', False).alias('phone1_ctry'),
                        get_xml_element('phone1', 'C_AREA_CODE', 'STRING', False).alias('phone1_area'),
                        get_xml_element('phone1', 'C_LOCAL', 'STRING', False).alias('phone1_local'),
                        get_xml_element('phone1', 'C_EXT', 'STRING', False).alias('phone1_ext'),
                        get_xml_element('phone2', 'C_CTRY_CODE', 'STRING', False).alias('phone2_ctry'),
                        get_xml_element('phone2', 'C_AREA_CODE', 'STRING', False).alias('phone2_area'),
                        get_xml_element('phone2', 'C_LOCAL', 'STRING', False).alias('phone2_local'),
                        get_xml_element('phone2', 'C_EXT', 'STRING', False).alias('phone2_ext'),
                        get_xml_element('phone3', 'C_CTRY_CODE', 'STRING', False).alias('phone3_ctry'),
                        get_xml_element('phone3', 'C_AREA_CODE', 'STRING', False).alias('phone3_area'),
                        get_xml_element('phone3', 'C_LOCAL', 'STRING', False).alias('phone3_local'),
                        get_xml_element('phone3', 'C_EXT', 'STRING', False).alias('phone3_ext'),
                        get_xml_attribute('customer','C_TIER','STRING'),
                    ) \
                    .select(
                        to_timestamp(col('action_ts'), lit('yyyy-mm-ddThh:mi:ss')).alias('action_ts'),
                        get_xml_attribute('$1','ActionType','STRING'),
                        get_xml_attribute('customer','C_ID','NUMBER'),
                        get_xml_attribute('customer','C_TAX_ID','STRING'),
                        get_xml_attribute('customer','C_GNDR','STRING'),
                        try_cast(col('c_tier'),'NUMBER').alias('c_tier'),
                        get_xml_attribute('customer','C_DOB','DATE'),
                        get_xml_element('name','C_L_NAME','STRING'),
                        get_xml_element('name','C_F_NAME','STRING'),
                        get_xml_element('name','C_M_NAME','STRING'),
                        get_xml_element('address','C_ADLINE1','STRING'),
                        get_xml_element('address', 'C_ADLINE2', 'STRING'),
                        get_xml_element('address','C_ZIPCODE','STRING'),
                        get_xml_element('address','C_CITY','STRING'),
                        get_xml_element('address','C_STATE_PROV','STRING'),
                        get_xml_element('address','C_CTRY','STRING'),
                        get_xml_element('contact_info','C_PRIM_EMAIL','STRING'),
                        get_xml_element('contact_info','C_ALT_EMAIL','STRING'),
                        get_phone_number('1'),
                        get_phone_number('2'),
                        get_phone_number('3'),
                        get_xml_element('tax_info','C_LCL_TX_ID','STRING'),
                        get_xml_element('tax_info','C_NAT_TX_ID','STRING'),
                        get_xml_attribute('account','CA_ID','STRING'),
                        get_xml_attribute('account','CA_TAX_ST','NUMBER'),
                        get_xml_element('account','CA_B_ID','NUMBER'),
                        get_xml_element('account','CA_NAME','STRING'),
                    )
        
                df = df.with_column_renamed("C_ID", "customerid") \
                            .with_column_renamed("CA_ID", "accountid") \
                            .with_column_renamed("CA_B_ID", "brokerid_NA") \
                            .with_column_renamed("C_TAX_ID", "taxid_NA") \
                            .with_column_renamed("CA_NAME", "accountdesc_NA") \
                            .with_column_renamed("CA_TAX_ST", "taxstatus_NA") \
                            .with_column_renamed("C_L_NAME", "lastname_NA") \
                            .with_column_renamed("C_F_NAME", "firstname_NA") \
                            .with_column_renamed("C_M_NAME", "middleinitial_NA") \
                            .with_column_renamed("C_GNDR", "gender_NA") \
                            .with_column_renamed("c_tier", "tier_NA") \
                            .with_column_renamed("C_DOB", "dob_NA") \
                            .with_column_renamed("C_ADLINE1", "addressline1_NA") \
                            .with_column_renamed("C_ADLINE2", "addressline2_NA") \
                            .with_column_renamed("C_ZIPCODE", "postalcode_NA") \
                            .with_column_renamed("C_CITY", "city_NA") \
                            .with_column_renamed("C_STATE_PROV", "stateprov_NA") \
                            .with_column_renamed("C_CTRY", "country_NA") \
                            .with_column_renamed("C_PHONE_1", "phone1_NA") \
                            .with_column_renamed("C_PHONE_2", "phone2_NA") \
                            .with_column_renamed("C_PHONE_3", "phone3_NA") \
                            .with_column_renamed("C_PRIM_EMAIL", "email1_NA") \
                            .with_column_renamed("C_ALT_EMAIL", "email2_NA") \
                            .with_column_renamed("C_LCL_TX_ID", "lcl_tx_id_NA") \
                            .with_column_renamed("C_NAT_TX_ID", "nat_tx_id_NA") \
                            .with_column_renamed(col("action_ts"), "update_ts") \
                            .with_column("status",(when(col("ActionType").isin("NEW", "ADDACCT", "UPDACCT", "UPDCUST"), "Active").otherwise("Inactive")))\
                            .with_column("taxid", iff(length(trim(col("taxid_NA")))>0,col("taxid_NA"),None ) ) \
                            .with_column("lastname", iff(length(trim(col("lastname_NA")))>0,col("lastname_NA"),None ) ) \
                            .with_column("firstname", iff(length(trim(col("firstname_NA")))>0,col("firstname_NA"),None ) ) \
                            .with_column("middleinitial", iff(length(trim(col("middleinitial_NA")))>0,col("middleinitial_NA"),None ) ) \
                            .with_column("gender", iff(length(trim(col("gender_NA")))>0,col("gender_NA"),None ) ) \
                            .with_column("tier", iff(length(trim(col("tier_NA")))>0,col("tier_NA"),None ) ) \
                            .with_column("dob", iff(length(trim(col("dob_NA")))>0,col("dob_NA"),None ) ) \
                            .with_column("addressline1", iff(length(trim(col("addressline1_NA")))>0,col("addressline1_NA"),None ) ) \
                            .with_column("addressline2", iff(length(trim(col("addressline2_NA")))>0,col("addressline2_NA"),None ) ) \
                            .with_column("postalcode", iff(length(trim(col("postalcode_NA")))>0,col("postalcode_NA"),None ) ) \
                            .with_column("CITY", iff(length(trim(col("CITY_NA")))>0,col("CITY_NA"),None ) ) \
                            .with_column("stateprov", iff(length(trim(col("stateprov_NA")))>0,col("stateprov_NA"),None ) ) \
                            .with_column("country", iff(length(trim(col("country_NA")))>0,col("country_NA"),None ) ) \
                            .with_column("phone1", iff(length(trim(col("phone1_NA")))>0,col("phone1_NA"),None ) ) \
                            .with_column("phone2", iff(length(trim(col("phone2_NA")))>0,col("phone2_NA"),None ) ) \
                            .with_column("phone3", iff(length(trim(col("phone3_NA")))>0,col("phone3_NA"),None ) ) \
                            .with_column("email1", iff(length(trim(col("email1_NA")))>0,col("email1_NA"),None ) ) \
                            .with_column("email2", iff(length(trim(col("email2_NA")))>0,col("email2_NA"),None ) ) \
                            .with_column("LCL_TX_ID", iff(length(trim(col("LCL_TX_ID_NA")))>0,col("LCL_TX_ID_NA"),None ) ) \
                            .with_column("NAT_TX_ID", iff(length(trim(col("NAT_TX_ID_NA")))>0,col("NAT_TX_ID_NA"),None ) ) \
                            .with_column("accountdesc", iff(length(trim(col("accountdesc_NA")))>0,col("accountdesc_NA"),None ) ) \
                            .with_column("taxstatus", iff(length(trim(col("taxstatus_NA")))>0,col("taxstatus_NA"),None ) ) \
                            .with_column("brokerid", iff(length(trim(col("brokerid_NA")))>0,col("brokerid_NA"),None ) ) \
                            .select("customerid", "accountid", "brokerid", "taxid", \
                                  "accountdesc", "taxstatus", "status", "lastname", "firstname", "middleinitial", \
                                  "gender", "tier", "dob", "addressline1", "addressline2", "postalcode", "city", \
                                  "stateprov", "country", "phone1", "phone2", "phone3", "email1", "email2", \
                                   "lcl_tx_id", "nat_tx_id", "update_ts", "ActionType")
                                   
                df.write.mode("overwrite").save_as_table('TPCDI_STG.BRONZE.CustomerMgmt')
                
                df_dummy = session.sql("""select
                        *
                    from
                        (
                        select
                            TPCDI_STG.REF.DimCustomer_SEQ.nextval as sk_customerid,
                            customerid,
                            
                            iff(length(taxid) > 0, taxid, iff(length(lag(taxid) ignore nulls over (partition by customerid order by update_ts))>0, lag(taxid) ignore nulls over (partition by customerid order by update_ts), NULL)) taxid,

                            status,
                            
                            iff(length(lastname) > 0, lastname, iff(length(lag(lastname) ignore nulls over (partition by customerid order by update_ts))>0, lag(lastname) ignore nulls over (partition by customerid order by update_ts), NULL)) lastname,
                            
                            iff(length(firstname) > 0, firstname, iff(length(lag(firstname) ignore nulls over (partition by customerid order by update_ts))>0, lag(firstname) ignore nulls over (partition by customerid order by update_ts), NULL)) firstname,
                            iff(length(middleinitial) > 0, middleinitial, iff(length(lag(middleinitial) ignore nulls over (partition by customerid order by update_ts))>0, lag(middleinitial) ignore nulls over (partition by customerid order by update_ts), NULL)) middleinitial,
                            iff(length(gender) > 0, gender, iff(length(lag(gender) ignore nulls over (partition by customerid order by update_ts))>0, lag(gender) ignore nulls over (partition by customerid order by update_ts), NULL)) gender,
                            iff(length(tier) > 0, tier, iff(length(lag(tier) ignore nulls over (partition by customerid order by update_ts))>0, lag(tier) ignore nulls over (partition by customerid order by update_ts), NULL)) tier,
                            iff(length(dob) > 0, dob, iff(length(lag(dob) ignore nulls over (partition by customerid order by update_ts))>0, lag(dob) ignore nulls over (partition by customerid order by update_ts), NULL)) dob,
                            iff(length(addressline1) > 0, addressline1, iff(length(lag(addressline1) ignore nulls over (partition by customerid order by update_ts))>0, lag(addressline1) ignore nulls over (partition by customerid order by update_ts), NULL)) addressline1,
                            iff(length(addressline2) > 0, addressline2, iff(length(lag(addressline2) ignore nulls over (partition by customerid order by update_ts))>0, lag(addressline2) ignore nulls over (partition by customerid order by update_ts), NULL)) addressline2,
                            iff(length(postalcode) > 0, postalcode, iff(length(lag(postalcode) ignore nulls over (partition by customerid order by update_ts))>0, lag(postalcode) ignore nulls over (partition by customerid order by update_ts), NULL)) postalcode,
                            iff(length(CITY) > 0, CITY, iff(length(lag(CITY) ignore nulls over (partition by customerid order by update_ts))>0, lag(CITY) ignore nulls over (partition by customerid order by update_ts), NULL)) CITY,
                            iff(length(stateprov) > 0, stateprov, iff(length(lag(stateprov) ignore nulls over (partition by customerid order by update_ts))>0, lag(stateprov) ignore nulls over (partition by customerid order by update_ts), NULL)) stateprov,
                            iff(length(country) > 0, country, iff(length(lag(country) ignore nulls over (partition by customerid order by update_ts))>0, lag(country) ignore nulls over (partition by customerid order by update_ts), NULL)) country,
                            iff(length(phone1) > 0, phone1, iff(length(lag(phone1) ignore nulls over (partition by customerid order by update_ts))>0, lag(phone1) ignore nulls over (partition by customerid order by update_ts), NULL)) phone1,
                            iff(length(phone2) > 0, phone2, iff(length(lag(phone2) ignore nulls over (partition by customerid order by update_ts))>0, lag(phone2) ignore nulls over (partition by customerid order by update_ts), NULL)) phone2,
                            iff(length(phone3) > 0, phone3, iff(length(lag(phone3) ignore nulls over (partition by customerid order by update_ts))>0, lag(phone3) ignore nulls over (partition by customerid order by update_ts), NULL)) phone3,
                            iff(length(email1) > 0, email1, iff(length(lag(email1) ignore nulls over (partition by customerid order by update_ts))>0, lag(email1) ignore nulls over (partition by customerid order by update_ts), NULL)) email1,
                            iff(length(email2) > 0, email2, iff(length(lag(email2) ignore nulls over (partition by customerid order by update_ts))>0, lag(email2) ignore nulls over (partition by customerid order by update_ts), NULL)) email2,
                            
                            
                            iff(length(LCL_TX_ID) > 0, LCL_TX_ID, iff(length(lag(LCL_TX_ID) ignore nulls over (partition by customerid order by update_ts))>0, lag(LCL_TX_ID) ignore nulls over (partition by customerid order by update_ts), NULL)) LCL_TX_ID,
                            
                            
                            iff(length(NAT_TX_ID) > 0, NAT_TX_ID, iff(length(lag(NAT_TX_ID) ignore nulls over (partition by customerid order by update_ts))>0, lag(NAT_TX_ID) ignore nulls over (partition by customerid order by update_ts), NULL)) NAT_TX_ID,


                            1 batchid,
                            nvl2(lead(update_ts) over (partition by customerid order by update_ts), false, true) iscurrent,
                            date(update_ts) effectivedate,
                            coalesce(lead(date(update_ts)) over (partition by customerid order by update_ts),
                            date('9999-12-31')) enddate
                        from
                            TPCDI_STG.BRONZE.CustomerMgmt c
                        where
                            ActionType in ('NEW', 'INACT', 'UPDCUST') )
                    where
                        effectivedate < enddate""") 
                
                df_dummy.write.mode("append").save_as_table("TPCDI_STG.SILVER.DimCustomerStg")
            ##-----------------------------------------------------------------------------------------------------------------------------
    
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## Table Name=TradeHistory : File Name=Batch1/Trade.txt
            ## Table Name=TradeHistoryRaw : File Name=Batch1/TradeHistory.txt
            ## For file14: TradeHistory.txt
            con_file_name = 'TradeHistory.txt'
            if file_name in ['all', con_file_name]:
                schema = StructType([
                        StructField("th_t_id", IntegerType(), False),
                        StructField("th_dts", TimestampType(), False),
                        StructField("th_st_id", StringType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.TradeHistoryRaw','|')
            ##-----------------------------------------------------------------------------------------------------------------------------
    
        ##-----------------------------------------------------------------------------------------------------------------------------
        
    
        ## For All Batch 
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## For file10: DailyMarket
        con_file_name = 'DailyMarket.txt'
        if file_name in ['all', con_file_name]:
            if batch == '1':
                schema = StructType([
                    StructField("dm_date", DateType(), False),
                    StructField("dm_s_symb", StringType(), False),
                    StructField("dm_close", FloatType(), False),
                    StructField("dm_high", FloatType(), False),
                    StructField("dm_low", FloatType(), False),
                    StructField("dm_vol", IntegerType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.V_DMH', '|')
            else:
                schema = StructType([
                StructField("cdc_flag", StringType(), False),
                StructField("cdc_dsn", IntegerType(), False),
                StructField("dm_date", DateType(), False),
                StructField("dm_s_symb", StringType(), False),
                StructField("dm_close", FloatType(), False),
                StructField("dm_high", FloatType(), False),
                StructField("dm_low", FloatType(), False),
                StructField("dm_vol", IntegerType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.DailyMarketIncremental', '|')
            
        ##-----------------------------------------------------------------------------------------------------------------------------
    
    
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## For file11: Prospect.csv
        con_file_name = 'Prospect.csv'
        if file_name in ['all', con_file_name]:
            schema = StructType([
                    StructField("agencyid", StringType(), False),
                    StructField("lastname", StringType(), True),
                    StructField("firstname", StringType(), True),
                    StructField("middleinitial", StringType(), True),
                    StructField("gender", StringType(), True),
                    StructField("addressline1", StringType(), True),
                    StructField("addressline2", StringType(), True),
                    StructField("postalcode", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("phone", StringType(), True),
                    StructField("income", IntegerType(), True),
                    StructField("numbercars", IntegerType(), True),
                    StructField("numberchildren", IntegerType(), True),
                    StructField("maritalstatus", StringType(), True),
                    StructField("age", IntegerType(), True),
                    StructField("creditrating", IntegerType(), True),
                    StructField("ownorrentflag", StringType(), True),
                    StructField("employer", StringType(), True),
                    StructField("numbercreditcards", IntegerType(), True),
                    StructField("networth", IntegerType(), True),
            ])
            load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.ProspectIncremental', ',')
            
        ##-----------------------------------------------------------------------------------------------------------------------------
    
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## For file12: WatchHistory.txt
        con_file_name = 'WatchHistory.txt'
        if file_name in ['all', con_file_name]:
            if batch == '1':
                schema = StructType([
                        StructField("w_c_id", IntegerType(), False),
                        StructField("w_s_symb", StringType(), True),
                        StructField("w_dts", TimestampType(), True),
                        StructField("w_action", StringType(), True)
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.watch_history', '|')
            else:
                schema = StructType([
                        StructField("cdc_flag", StringType(), False),
                        StructField("cdc_dsn", IntegerType(), False),
                        StructField("w_c_id", IntegerType(), False),
                        StructField("w_s_symb", StringType(), True),
                        StructField("w_dts", TimestampType(), True),
                        StructField("w_action", StringType(), True)
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.WatchIncremental', '|')
        ##-----------------------------------------------------------------------------------------------------------------------------
    
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## Table Name=TradeHistory : File Name=Batch1/Trade.txt
        ## Table Name=TradeHistoryRaw : File Name=Batch1/TradeHistory.txt
        ## For file13: Trade.txt
            con_file_name = 'Trade.txt'
        if file_name in ['all', con_file_name]:
            if batch == '1':
                schema = StructType([
                        StructField("t_id", IntegerType(), False),
                        StructField("t_dts", TimestampType(), False),
                        StructField("t_st_id", StringType(), False),
                        StructField("t_tt_id", StringType(), False),
                        StructField("t_is_cash", BooleanType(), False),
                        StructField("t_s_symb", StringType(), False),
                        StructField("t_qty", IntegerType(),False),
                        StructField("t_bid_price", FloatType(), False),
                        StructField("t_ca_id", IntegerType(), False),
                        StructField("t_exec_name", StringType(), False),
                        StructField("t_trade_price", FloatType(), True),
                        StructField("t_chrg", FloatType(), True),
                        StructField("t_comm", FloatType(), True),
                        StructField("t_tax", FloatType(), True),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.TradeHistory', '|')
            else:
                schema = StructType([
                        StructField("cdc_flag", StringType(), False),
                        StructField("cdc_dsn", IntegerType(), False),
                        StructField("t_id", IntegerType(), False),
                        StructField("t_dts", TimestampType(), False),
                        StructField("t_st_id", StringType(), False),
                        StructField("t_tt_id", StringType(), False),
                        StructField("t_is_cash", IntegerType(), False),
                        StructField("t_s_symb", StringType(), False),
                        StructField("t_qty", IntegerType(),False),
                        StructField("t_bid_price", FloatType(), False),
                        StructField("t_ca_id", IntegerType(), False),
                        StructField("t_exec_name", StringType(), False),
                        StructField("t_trade_price", FloatType(), True),
                        StructField("t_chrg", FloatType(), True),
                        StructField("t_comm", FloatType(), True),
                        StructField("t_tax", FloatType(), True),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.TradeIncremental', '|')
        ##-----------------------------------------------------------------------------------------------------------------------------
    
        
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## For file15: CashTransaction.txt
        con_file_name = 'CashTransaction.txt'
        if file_name in ['all', con_file_name]:
            if batch == '1':
                schema = StructType([
                    StructField("ct_ca_id", IntegerType(), False),
                    StructField("ct_dts", TimestampType(), False),
                    StructField("ct_amt", FloatType(), False),
                    StructField("ct_name", StringType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.CashTransaction','|')
            else:
                schema = StructType([
                    StructField("cdc_flag", StringType(), False),
                    StructField("cdc_dsn", IntegerType(), False),
                    StructField("ct_ca_id", IntegerType(), False),
                    StructField("ct_dts", TimestampType(), False),
                    StructField("ct_amt", FloatType(), False),
                    StructField("ct_name", StringType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.CashTransactionIncremental','|')
        ##-----------------------------------------------------------------------------------------------------------------------------
    
        ##-----------------------------------------------------------------------------------------------------------------------------
        ## For file16: HoldingHistory.txt
        con_file_name = 'HoldingHistory.txt'
        if file_name in ['all', con_file_name]:
            if batch == '1':
                schema = StructType([
                    StructField("hh_h_t_id", IntegerType(), False),
                    StructField("hh_t_id", IntegerType(), False),
                    StructField("hh_before_qty", FloatType(), False),
                    StructField("hh_after_qty", FloatType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.holding_history','|')
            else:
                schema = StructType([
                    StructField("cdc_flag", StringType(), False),
                    StructField("cdc_dsn", IntegerType(), False),
                    StructField("hh_h_t_id", IntegerType(), False),
                    StructField("hh_t_id", IntegerType(), False),
                    StructField("hh_before_qty", FloatType(), False),
                    StructField("hh_after_qty", FloatType(), False),
                ])
                load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.HoldingIncremental','|')
        ##-----------------------------------------------------------------------------------------------------------------------------
    
        if batch != '1':
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file17: Account.txt
            con_file_name = 'Account.txt'
            if file_name in ['all', con_file_name]:
                    schema = StructType([
                        StructField("cdc_flag", StringType(), False),
                        StructField("cdc_dsn", IntegerType(), False),
                        StructField("accountid", IntegerType(), False),
                        StructField("ca_b_id", IntegerType(), False),
                        StructField("ca_c_id", IntegerType(), False),
                        StructField("accountDesc", StringType(), False),
                        StructField("TaxStatus", IntegerType(), False),
                        StructField("ca_st_id", StringType(), False),
                    ])
                    load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.AccountIncremental','|')
            ##-----------------------------------------------------------------------------------------------------------------------------
    
            ##-----------------------------------------------------------------------------------------------------------------------------
            ## For file18: Customer.txt
            con_file_name = 'Customer.txt'
            if file_name in ['all', con_file_name]:
                    schema = StructType([
                        StructField("cdc_flag", StringType(), False),
                        StructField("cdc_dsn", IntegerType(), False),                    
                        StructField("customerid", IntegerType(), False),
                        StructField("taxid", StringType(), False),
                        StructField("status", StringType(), False),
                        StructField("lastname", StringType(), False),
                        StructField("firstname", StringType(), False),
                        StructField("middleinitial", StringType(), False),
                        StructField("gender", StringType(), False),
                        StructField("tier", IntegerType(), False),
                        StructField("dob", DateType(), False),
                        
                        StructField("addressline1", StringType(), False),
                        StructField("addressline2", StringType(), False),
                        StructField("postalcode", StringType(), False),
                        StructField("city", StringType(), False),
                        StructField("stateprov", StringType(), False),
                        StructField("country", StringType(), False),
                        StructField("c_ctry_1", StringType(), False),
                        StructField("c_area_1", StringType(), False),
                        StructField("c_local_1", StringType(), False),
                        StructField("c_ext_1", StringType(), False),
                        
                        
                        StructField("c_ctry_2", StringType(), False),
                        StructField("c_area_2", StringType(), False),
                        StructField("c_local_2", StringType(), False),
                        StructField("c_ext_2", StringType(), False),
                        StructField("c_ctry_3", StringType(), False),
                        StructField("c_area_3", StringType(), False),
                        StructField("c_local_3", StringType(), False),
                        StructField("c_ext_3", StringType(), False),
                        StructField("email1", StringType(), False),
                        StructField("email2", StringType(), False),
                        StructField("lcl_tx_id", StringType(), False),
                        StructField("nat_tx_id", StringType(), False),
                        StructField("batchid", IntegerType(), False),
                        
                    ])
                    load_csv_table(schema, con_file_name, 'TPCDI_STG.BRONZE.CustomerIncremental','|')
            ##-----------------------------------------------------------------------------------------------------------------------------
    
                
        ##-----------------------------------------------------------------------------------------------------------------------------   
        ## creating Silver layer tables
    
        if batch == '1':    
            ##-----------------------------------------------------------------------------------------------------------------------------   
            ## DimBroker
            session.sql("SELECT TPCDI_STG.REF.DimBroker_SEQ.nextval AS sk_brokerid, cast(employeeid as BIGINT), cast(managerid as BIGINT), employeefirstname, employeelastname, employeemi, employeebranch, employeeoffice, employeephone, 'True' AS iscurrent, '1' AS batchid, (SELECT min(to_date(datevalue)) FROM TPCDI_WH.BRONZE.DimDate), date('9999-12-31') FROM TPCDI_STG.BRONZE.HR WHERE employeejobcode = 314") \
            .write.mode("append").save_as_table('TPCDI_WH.SILVER.DimBroker')
            ##-----------------------------------------------------------------------------------------------------------------------------   
    
            ##-----------------------------------------------------------------------------------------------------------------------------   
            ## DimCompany
            session.sql("SELECT * FROM ( SELECT TPCDI_STG.REF.DimCompany_SEQ.nextval AS sk_companyid, cast (cik as string) companyid,   st.st_name status,   companyname name,   ind.in_name industry, iff(SPrating IN ('AAA ','AA+ ','AA- ','A+  ','A-  ','AA  ','A   ','BBB ','BB- ','BB+ ','B+  ','B-  ','BB  ','B   ','BBB+','BBB-','CCC-','CCC+','CCC ','CC  ','C   ','D   '),SPrating, cast(null as string)) sprating, CASE WHEN SPrating IN ('AAA ','AA+ ','AA- ','A+  ','A-  ','AA  ','A   ','BBB ','BBB+','BBB-') THEN false WHEN SPrating IN ('BB  ','B   ','CCC ','CC  ','C   ','D   ','BB+ ','B+  ','CCC+','BB- ','B-  ','CCC-') THEN true ELSE cast(null as boolean) END as islowgrade, ceoname ceo, addrline1 addressline1, addrline2 addressline2, postalcode, city, stateprovince stateprov, country, description, case when trim(substr(foundingdate, 1,8)) ilike '' or trim(substr(foundingdate, 1,8)) ilike '0' then null else concat(substr(foundingdate, 1,4),'-',substr(foundingdate, 5,2),'-',substr(foundingdate, 7,2)) end as foundingdate, nvl2(lead(pts) OVER (PARTITION BY cik ORDER BY pts), true, false) iscurrent, 1 batchid, date(pts) effectivedate, coalesce( lead(date(pts)) OVER (   PARTITION BY cik   ORDER BY pts), cast('9999-12-31' as date)) enddate FROM TPCDI_STG.BRONZE.FinwireCmpView cmp JOIN TPCDI_WH.BRONZE.StatusType st ON cmp.status = st.st_id JOIN TPCDI_WH.BRONZE.Industry ind ON cmp.industryid = ind.in_id ) WHERE effectivedate < enddate") \
            .write.mode("append").save_as_table('TPCDI_WH.SILVER.DimCompany')
            ##----------------------------------------------------------------------------------------------------------------------------- 
          
            ##----------------------------------------------------------------------------------------------------------------------------- 
            ## DimSecurity
            session.sql("SELECT fws.*, nvl(TRY_CAST(trim(conameorcik) AS int )::string,TRY_CAST(trim(conameorcik) AS string )::string) conameorcik_New, s.ST_NAME as status_New, coalesce(lead(effectivedate) OVER (PARTITION BY symbol ORDER BY effectivedate), date('9999-12-31') ) enddate FROM TPCDI_STG.BRONZE.FinwireSecView fws JOIN TPCDI_WH.BRONZE.StatusType s ON s.ST_ID = fws.status") \
            .drop(col('status'), col('conameorcik')) \
            .withColumn('conameorcik', col('conameorcik_New')) \
            .withColumn('status', col('status_New')) \
            .createOrReplaceTempView("TPCDI_STG.silver.FinwireSecStg")
            
            session.sql("select  TPCDI_STG.REF.DimSecurity_SEQ.nextval AS sk_securityid,fws.Symbol,fws.issue,fws.status,fws.Name,fws.exchangeid,dc.sk_companyid, fws.sharesoutstanding,case when trim(substr(fws.firsttrade, 1,8)) ilike '' or trim(substr(fws.firsttrade, 1,8)) ilike '0' then null else concat(substr(fws.firsttrade, 1,4),'-',substr(fws.firsttrade, 5,2),'-',substr(fws.firsttrade, 7,2)) end AS firsttrade,case when trim(substr(fws.firsttradeonexchange, 1,8)) ilike '' or trim(substr(fws.firsttradeonexchange, 1,8)) ilike '0' then null else concat(substr(fws.firsttradeonexchange, 1,4),'-',substr(fws.firsttradeonexchange, 5,2),'-',substr(fws.firsttradeonexchange, 7,2)) end AS firsttradeonexchange,fws.Dividend,iff(fws.effectivedate < dc.effectivedate, dc.effectivedate, fws.effectivedate) effectivedate, iff(fws.enddate > dc.enddate, dc.enddate, fws.enddate) enddate FROM TPCDI_STG.silver.FinwireSecStg fws JOIN (SELECT sk_companyid,trim(name) conameorcik, EffectiveDate, EndDate FROM TPCDI_WH.SILVER.DimCompany UNION ALL SELECT sk_companyid, cast(companyid as string) conameorcik, EffectiveDate,EndDate FROM TPCDI_WH.SILVER.DimCompany ) dc ON TRIM(fws.conameorcik) = TRIM(dc.conameorcik) AND fws.EffectiveDate < dc.EndDate AND fws.EndDate > dc.EffectiveDate") \
            .withColumn('iscurrent', when(col('enddate')==('9999-12-31'), lit(True)).otherwise(lit(False)) ) \
            .withColumn('batchid', lit('1') ) \
            .select('sk_securityid','Symbol','issue','status','Name','exchangeid','sk_companyid','sharesoutstanding','firsttrade','firsttradeonexchange','Dividend','iscurrent','batchid','effectivedate','enddate') \
            .write.mode("append").save_as_table('TPCDI_WH.SILVER.DimSecurity')
            ##----------------------------------------------------------------------------------------------------------------------------- 
    
            ##----------------------------------------------------------------------------------------------------------------------------- 
            ## Financial
            session.sql("SELECT fws.*, nvl(TRY_CAST(trim(conameorcik) AS int )::string,TRY_CAST(trim(conameorcik) AS string )::string) conameorcik_New FROM TPCDI_STG.BRONZE.FinwireFinView fws") \
            .drop(col('conameorcik')) \
            .withColumn('conameorcik', col('conameorcik_New')) \
            .createOrReplaceTempView("TPCDI_STG.silver.FinwireFinStg")
            
            session.sql("SELECT sk_companyid, fi_year, fi_qtr, fi_qtr_start_date, fi_revenue, fi_net_earn, fi_basic_eps, fi_dilut_eps, fi_margin, fi_inventory, fi_assets, fi_liability, fi_out_basic, fi_out_dilut FROM TPCDI_STG.silver.FinwireFinStg f JOIN ( SELECT sk_companyid, name conameorcik, EffectiveDate, EndDate FROM TPCDI_WH.SILVER.DimCompany UNION ALL SELECT sk_companyid, cast(companyid as string) conameorcik, EffectiveDate, EndDate FROM TPCDI_WH.SILVER.DimCompany) dc ON TRIM(f.conameorcik) = TRIM(dc.conameorcik) AND PTS >= dc.effectivedate AND PTS < dc.enddate") \
            .write.mode("append").save_as_table('TPCDI_WH.SILVER.Financial')
            ##----------------------------------------------------------------------------------------------------------------------------- 
            
    
        
        
        ##----------------------------------------------------------------------------------------------------------------------------- 
        ## Prospect
        session.sql("delete from TPCDI_WH.SILVER.PROSPECT").collect()
        
        session.sql(f"SELECT agencyid, recdate.sk_dateid sk_recorddateid, origdate.sk_dateid sk_updatedateid, p.batchid, nvl2(c.lastname, True, False) iscustomer, p.lastname, p.firstname, p.middleinitial, p.gender, p.addressline1, p.addressline2, p.postalcode, city, state, country, phone, income, numbercars, numberchildren, maritalstatus, age, creditrating, ownorrentflag, employer, numbercreditcards, networth, NVL(left( iff(networth > 1000000 or income > 200000,'HighValue+','') || iff(numberchildren > 3 or numbercreditcards > 5,'Expenses+','') || iff(age > 45, 'Boomer+', '') || iff(income < 50000 or creditrating < 600 or networth < 100000, 'MoneyAlert+','') || iff(numbercars > 3 or numbercreditcards > 7, 'Spender+','') || iff(age < 25 and networth > 1000000, 'Inherited+',''), length( iff(networth > 1000000 or income > 200000,'HighValue+','') || iff(numberchildren > 3 or numbercreditcards > 5,'Expenses+','') || iff(age > 45, 'Boomer+', '') || iff(income < 50000 or creditrating < 600 or networth < 100000, 'MoneyAlert+','') || iff(numbercars > 3 or numbercreditcards > 7, 'Spender+','') || iff(age < 25 and networth > 1000000, 'Inherited+','')) -1), NULL) marketingnameplate FROM ( SELECT * FROM ( SELECT agencyid, max(batchid) recordbatchid, lastname, firstname, middleinitial, gender, addressline1, addressline2, postalcode, city, state, country, phone, income, numbercars, numberchildren, maritalstatus, age, creditrating, ownorrentflag, employer, numbercreditcards, networth, min(batchid) batchid FROM TPCDI_STG.BRONZE.ProspectIncremental p WHERE batchid <= cast({batch} as int) GROUP BY agencyid, lastname, firstname, middleinitial, gender, addressline1, addressline2, postalcode, city, state, country, phone, income, numbercars, numberchildren, maritalstatus, age, creditrating, ownorrentflag, employer, numbercreditcards, networth) QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1) p JOIN ( SELECT sk_dateid, batchid FROM TPCDI_WH.BRONZE.BatchDate b JOIN TPCDI_WH.BRONZE.DimDate d ON b.batchdate = d.datevalue) recdate ON p.recordbatchid = recdate.batchid JOIN ( SELECT sk_dateid, batchid FROM TPCDI_WH.BRONZE.BatchDate b JOIN TPCDI_WH.BRONZE.DimDate d ON b.batchdate = d.datevalue) origdate ON p.batchid = origdate.batchid  LEFT JOIN ( SELECT lastname, firstname, addressline1, addressline2, postalcode FROM TPCDI_STG.SILVER.DIMCUSTOMERSTG WHERE iscurrent) c ON upper(p.LastName) = upper(c.lastname) and upper(p.FirstName) = upper(c.firstname) and upper(p.AddressLine1) = upper(c.addressline1) and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, '')) and upper(p.PostalCode) = upper(c.postalcode)") \
        .write.mode("append").save_as_table('TPCDI_WH.SILVER.PROSPECT')
        
        ##----------------------------------------------------------------------------------------------------------------------------- 
		
		
		##----------------------------------------------------------------------------------------------------------------------------- 
        ## DimCustomer
        if batch != '1':
            
            incr_query = f"SELECT c.customerid, nullif(c.taxid, '') taxid, nullif(s.st_name, '') as status, nullif(c.lastname, '') lastname, nullif(c.firstname, '') firstname, nullif(c.middleinitial, '') middleinitial, gender, c.tier, c.dob, nullif(c.addressline1, '') addressline1, nullif(c.addressline2, '') addressline2, nullif(c.postalcode, '') postalcode, nullif(c.city, '') city, nullif(c.stateprov, '') stateprov, nullif(c.country, '') country, CASE WHEN c_local_1 is null then c_local_1 ELSE concat(   nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),   nvl2(c_area_1, '(' || c_area_1 || ') ', ''),   c_local_1,   nvl(c_ext_1, '')) END as phone1, CASE WHEN c_local_2 is null then c_local_2 ELSE concat(   nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),   nvl2(c_area_2, '(' || c_area_2 || ') ', ''), c_local_2, nvl(c_ext_2, '')) END as phone2, CASE WHEN c_local_3 is null then c_local_3 ELSE concat( nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''), nvl2(c_area_3, '(' || c_area_3 || ') ', ''), c_local_3, nvl(c_ext_3, '')) END as phone3, nullif(c.email1, '') email1, nullif(c.email2, '') email2, c.LCL_TX_ID, c.NAT_TX_ID, c.batchid, true iscurrent, bd.batchdate effectivedate, date('9999-12-31') enddate FROM TPCDI_STG.BRONZE.CustomerIncremental c  JOIN TPCDI_WH.BRONZE.BatchDate bd ON c.batchid = bd.batchid JOIN TPCDI_WH.BRONZE.StatusType s ON c.status = s.st_id WHERE c.batchid = cast({batch} as int)"
    
            tgt_cols = "customerid, taxid, status, lastname, firstname, middleinitial, gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, country, phone1, phone2, phone3, email1, email2, lcl_tx_id, nat_tx_id, batchid, iscurrent, effectivedate, enddate"
            
            session.sql(f"""MERGE INTO TPCDI_STG.SILVER.DimCustomerStg t USING (
                  SELECT DISTINCT
                    s.customerid AS mergeKey,
                    s.*
                  FROM ({incr_query}) s
                  JOIN TPCDI_STG.SILVER.DimCustomerStg t
                    ON s.customerid = t.customerid
                  WHERE t.iscurrent
                  UNION ALL
                  SELECT DISTINCT
                    cast(null as bigint) AS mergeKey,
                    *
                  FROM ({incr_query})
                ) s 
                  ON 
                    t.customerid = s.mergeKey
                    AND t.iscurrent
                WHEN MATCHED THEN UPDATE SET
                  t.iscurrent = false,
                  t.enddate = s.effectivedate
                WHEN NOT MATCHED THEN INSERT (sk_customerid,{tgt_cols})
                VALUES (TPCDI_STG.REF.DimCustomer_SEQ.nextval,{tgt_cols})""").collect()  
        
        
        session.sql("delete from TPCDI_WH.SILVER.DimCustomer").collect()
        
        session.sql("SELECT sk_customerid, c.customerid, c.taxid, c.status, c.lastname, c.firstname, c.middleinitial, iff(upper(trim(c.gender)) IN ('M', 'F'), upper(trim(c.gender)), 'U') gender, c.tier, c.dob, c.addressline1, c.addressline2, c.postalcode, c.city, c.stateprov, c.country, c.phone1, c.phone2, c.phone3, c.email1, c.email2, r_nat.TX_NAME as nationaltaxratedesc, r_nat.TX_RATE as nationaltaxrate, r_lcl.TX_NAME as localtaxratedesc, r_lcl.TX_RATE as localtaxrate, p.agencyid, p.creditrating, p.networth, p.marketingnameplate, c.iscurrent, c.batchid, c.effectivedate, c.enddate FROM TPCDI_STG.SILVER.DimCustomerStg c JOIN TPCDI_WH.BRONZE.TaxRate r_lcl ON c.LCL_TX_ID = r_lcl.TX_ID JOIN TPCDI_WH.BRONZE.TaxRate r_nat ON c.NAT_TX_ID = r_nat.TX_ID LEFT JOIN TPCDI_WH.SILVER.PROSPECT p on upper(p.lastname) = upper(c.lastname) and upper(p.firstname) = upper(c.firstname) and upper(p.addressline1) = upper(c.addressline1) and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, '')) and upper(p.postalcode) = upper(c.postalcode)") \
        .write.mode("append").save_as_table('TPCDI_WH.SILVER.DimCustomer')
        
        ##----------------------------------------------------------------------------------------------------------------------------- 
		
		##----------------------------------------------------------------------------------------------------------------------------- 
        ## DimAccount
        if batch == '1':
            session.sql(f"""select
                    TPCDI_STG.REF.DimAccount_SEQ.nextval as sk_accountid,
                    a.accountid,
                    b.sk_brokerid,
                    a.sk_customerid,
                    a.accountdesc,
                    a.TaxStatus,
                    a.status,
                    iff(a.enddate = date('9999-12-31'),
                    true,
                    false) as iscurrent,
                    1 batchid,
                    a.effectivedate,
                    a.enddate
                from
                    (
                    select
                        a.* exclude(effectivedate,
                        enddate,
                        customerid),
                        c.sk_customerid,
                        iff(a.effectivedate < c.effectivedate,
                        c.effectivedate,
                        a.effectivedate) effectivedate,
                        iff(a.enddate > c.enddate,
                        c.enddate,
                        a.enddate) enddate
                    from
                        (
                        select
                            *
                        from
                            (
                            select
                                accountid,
                                customerid,
                                
                                iff(length(accountdesc) > 0, accountdesc, iff(length(lag(accountdesc) ignore nulls over (partition by accountid order by update_ts))>0, lag(accountdesc) ignore nulls over (partition by accountid order by update_ts), NULL)) accountdesc,
                                
                                iff(length(taxstatus) > 0, taxstatus, iff(length(lag(taxstatus) ignore nulls over (partition by accountid order by update_ts))>0, lag(taxstatus) ignore nulls over (partition by accountid order by update_ts), NULL)) taxstatus,
                                
                                iff(length(brokerid) > 0, brokerid, iff(length(lag(brokerid) ignore nulls over (partition by accountid order by update_ts))>0, lag(brokerid) ignore nulls over (partition by accountid order by update_ts), NULL)) brokerid,
                                
                                iff(length(status) > 0, status, iff(length(lag(status) ignore nulls over (partition by accountid order by update_ts))>0, lag(status) ignore nulls over (partition by accountid order by update_ts), NULL)) status,
                                
                                date(update_ts) effectivedate,
                                nvl(lead(date(update_ts)) over (partition by accountid order by update_ts), date('9999-12-31')) enddate
                            from
                                TPCDI_STG.BRONZE.CustomerMgmt
                            where
                                ActionType not in ('UPDCUST', 'INACT'))
                        where
                            effectivedate < enddate) a
                    full outer join (
                        select
                            sk_customerid,
                            customerid,
                            effectivedate,
                            enddate
                        from
                            TPCDI_STG.SILVER.DimCustomerStg
                        where
                            batchid = cast({batch} as int) ) c on
                        a.customerid = c.customerid
                        and c.enddate > a.effectivedate
                        and c.effectivedate < a.enddate ) a
                left join TPCDI_WH.SILVER.DimBroker b on
                    a.brokerid = b.brokerid""").write.mode("append").save_as_table('TPCDI_WH.SILVER.DimAccount')
                    
        else:
            
            tgt_cols = "accountid, sk_brokerid, sk_customerid, accountdesc, TaxStatus, status, batchid, effectivedate, enddate"
            
            cust_updates_query = f""" SELECT DISTINCT
                a.accountid,
                a.sk_brokerid,
                ci.sk_customerid,
                a.status,    
                a.accountdesc,
                a.taxstatus,
                ci.effectivedate,
                ci.enddate
              FROM (
                SELECT 
                  sk_customerid, 
                  customerid,
                  effectivedate,
                  enddate
                FROM TPCDI_STG.SILVER.DimCustomerStg
                WHERE 
                  iscurrent 
                  AND batchid = cast({batch} as int)) ci
              JOIN (
                SELECT 
                  sk_customerid, 
                  customerid,
                  enddate
                FROM TPCDI_STG.SILVER.DimCustomerStg
                WHERE 
                  not iscurrent
                  AND batchid < cast({batch} as int)) ch
              ON 
                ci.customerid = ch.customerid
                AND ch.enddate = ci.effectivedate  
              JOIN TPCDI_WH.SILVER.DimAccount a
                ON 
                  ch.sk_customerid = a.sk_customerid
                  AND a.iscurrent"""
            
            incr_acct_query = f"""SELECT DISTINCT
                    nvl(a.accountid, b.accountid) accountid,
                    nvl(a.sk_brokerid, b.sk_brokerid) sk_brokerid,
                    nvl(a.sk_customerid, b.sk_customerid) sk_customerid,
                    nvl(a.status, b.status) status,
                    nvl(a.accountdesc, b.accountdesc) accountdesc,
                    nvl(a.TaxStatus, b.TaxStatus) TaxStatus,
                    cast({batch} as int) batchid,
                    nvl(a.effectivedate, b.effectivedate) effectivedate,
                    nvl(a.enddate, b.enddate) enddate
                  FROM (
                    SELECT
                      accountid,
                      b.sk_brokerid,
                      dc.sk_customerid,
                      st_name as status,
                      accountDesc,
                      TaxStatus,
                      bd.batchdate effectivedate,
                      date('9999-12-31') enddate,
                      a.batchid
                    FROM TPCDI_STG.BRONZE.AccountIncremental a
                    JOIN TPCDI_WH.BRONZE.BatchDate bd
                      ON a.batchid = bd.batchid
                    JOIN TPCDI_WH.BRONZE.StatusType st 
                      ON a.CA_ST_ID = st.st_id
                    JOIN (
                      SELECT customerid, sk_customerid
                      FROM TPCDI_STG.SILVER.DimCustomerStg
                      WHERE iscurrent) dc
                      ON dc.customerid = a.ca_c_id
                    LEFT JOIN TPCDI_WH.SILVER.DimBroker b 
                      ON a.ca_b_id = b.brokerid
                    WHERE a.batchid = cast({batch} as int)
                  ) a
                  FULL OUTER JOIN ({cust_updates_query}) b
                    ON a.accountid = b.accountid"""
    
            
            matched_accts_query = f"""SELECT DISTINCT
                    s.accountid mergeKey,
                    s.accountid,
                    nvl(s.sk_brokerid, t.sk_brokerid) sk_brokerid,
                    s.sk_customerid,
                    s.status,
                    
                    nvl(s.accountdesc, t.accountdesc) accountdesc,
                    nvl(s.taxstatus, t.taxstatus) taxstatus,
                    s.batchid,
                    s.effectivedate,
                    s.enddate    
                  FROM ({incr_acct_query}) s
                  JOIN TPCDI_WH.SILVER.DimAccount t
                    ON s.accountid = t.accountid
                  WHERE t.iscurrent"""
            
            session.sql(f"""MERGE INTO TPCDI_WH.SILVER.DimAccount t USING (
              SELECT
                CAST(NULL AS BIGINT) AS mergeKey,
                dav.*
              FROM ({incr_acct_query}) dav
              UNION ALL
              SELECT *
              FROM ({matched_accts_query})) s 
              ON t.accountid = s.mergeKey
            WHEN MATCHED AND t.iscurrent THEN UPDATE SET
              t.iscurrent = false,
              t.enddate = s.effectivedate
            WHEN NOT MATCHED THEN INSERT (SK_ACCOUNTID, {tgt_cols}, iscurrent )
            VALUES (TPCDI_STG.REF.DimAccount_SEQ.nextval,{tgt_cols}, iff(enddate = date('9999-12-31'), true, false) )""").collect()
                    
        ##----------------------------------------------------------------------------------------------------------------------------- 

		
        ##----------------------------------------------------------------------------------------------------------------------------- 
        ## FactCashBalances
        session.sql("SELECT accountid, datevalue, sum(account_daily_total) OVER (partition by accountid order by datevalue) cash, batchid FROM ( SELECT accountid, datevalue, sum(ct_amt) account_daily_total, batchid FROM ( SELECT ct_ca_id accountid, to_date(ct_dts) datevalue, ct_amt, batchid FROM TPCDI_STG.BRONZE.CashTransaction UNION ALL SELECT ct_ca_id accountid, to_date(ct_dts) datevalue, ct_amt, batchid FROM TPCDI_STG.BRONZE.CashTransactionIncremental ) GROUP BY accountid, datevalue, batchid )") \
        .createOrReplaceTempView("TPCDI_STG.SILVER.FactCashBalancesStg")
        
        session.sql(f"SELECT sk_customerid, sk_accountid, sk_dateid, fcb.cash, fcb.batchid FROM TPCDI_STG.SILVER.FactCashBalancesStg fcb JOIN TPCDI_WH.BRONZE.DimDate d ON fcb.datevalue = d.datevalue LEFT JOIN TPCDI_WH.SILVER.DimAccount a ON fcb.accountid = a.accountid AND fcb.datevalue >= a.effectivedate AND fcb.datevalue < a.enddate  WHERE fcb.batchid = cast({batch} as int)") \
        .write.mode("append").save_as_table('TPCDI_WH.GOLD.FactCashBalances')
        ##----------------------------------------------------------------------------------------------------------------------------- 
        
    
        
        ##----------------------------------------------------------------------------------------------------------------------------- 
        ## DIMTrade
        
        if batch == '1':
            session.sql("SELECT trade.tradeid, sk_brokerid, trade.sk_createdateid, trade.sk_createtimeid, trade.sk_closedateid, trade.sk_closetimeid, st_name status, tt_name type, trade.cashflag, sk_securityid, sk_companyid, trade.quantity, trade.bidprice, sk_customerid, sk_accountid, trade.executedby, trade.tradeprice, trade.fee, trade.commission, trade.tax, trade.batchid  FROM (select * exclude(t_dts, createdate), nvl2(sk_createdateid, createdate, cast(null as timestamp)) createdate from ( select tradeid, min(date(t_dts)) over (partition by tradeid) createdate, t_dts, coalesce(sk_createdateid, first_value(sk_createdateid) ignore nulls over ( partition by tradeid order by t_dts)) sk_createdateid, coalesce(sk_createtimeid, first_value(sk_createtimeid) ignore nulls over ( partition by tradeid order by t_dts)) sk_createtimeid, coalesce(sk_closedateid, last_value(sk_closedateid) ignore nulls over ( partition by tradeid order by t_dts)) sk_closedateid, coalesce(sk_closetimeid, last_value(sk_closetimeid) ignore nulls over ( partition by tradeid order by t_dts)) sk_closetimeid, cashflag, t_st_id, t_tt_id, t_s_symb, quantity, bidprice, t_ca_id, executedby, tradeprice, fee, commission, tax, batchid from ( select tradeid, t_dts, iff(create_flg, sk_dateid, cast(null as BIGINT)) sk_createdateid, iff(create_flg, sk_timeid, cast(null as BIGINT)) sk_createtimeid, iff(not create_flg, sk_dateid, cast(null as BIGINT)) sk_closedateid, iff(not create_flg, sk_timeid, cast(null as BIGINT)) sk_closetimeid, case when t_is_cash = 1 then true when t_is_cash = 0 then false else cast(null as BOOLEAN) end as cashflag, t_st_id, t_tt_id, t_s_symb, quantity, bidprice, t_ca_id, executedby, tradeprice, fee, commission, tax, t.batchid from ( select t_id tradeid, th_dts as t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty as quantity, t_bid_price as bidprice, t_ca_id, t_exec_name as executedby, t_trade_price as tradeprice, t_chrg as fee, t_comm as commission, t_tax as tax, batchid, case when ((UPPER(th_st_id) = 'SBMT') and UPPER(t_tt_id) in ('TMB', 'TMS')) or (UPPER(th_st_id) = 'PNDG') then true when UPPER(th_st_id) in ('CMPT', 'CNCL') then false else cast(null as boolean) end as create_flg from TPCDI_STG.BRONZE.TradeHistory t join TPCDI_STG.BRONZE.TradeHistoryRaw th on th_t_id = t_id) t join TPCDI_WH.BRONZE.DimDate dd on date(t.t_dts) = dd.datevalue join TPCDI_WH.BRONZE.DimTime dt on TO_TIME(t_dts) = dt.timevalue ) ) QUALIFY row_number() over (partition by tradeid order by t_dts desc) = 1) trade  JOIN TPCDI_WH.BRONZE.StatusType status ON status.st_id = trade.t_st_id  JOIN TPCDI_WH.BRONZE.TradeType tt ON tt.tt_id = trade.t_tt_id LEFT JOIN TPCDI_WH.SILVER.DimSecurity ds ON ds.symbol = trade.t_s_symb AND createdate >= ds.effectivedate AND createdate < ds.enddate  LEFT JOIN TPCDI_WH.SILVER.DimAccount da ON trade.t_ca_id = da.accountid AND createdate >= da.effectivedate AND createdate < da.enddate") \
            .write.mode("append").save_as_table('TPCDI_WH.SILVER.DimTrade')
        else:
            tgt_cols = "tradeid, sk_brokerid, sk_createdateid, sk_createtimeid, sk_closedateid, sk_closetimeid, status, type, cashflag, sk_securityid, sk_companyid, quantity, bidprice, sk_customerid, sk_accountid, executedby, tradeprice, fee, commission, tax, batchid"
            
            trade_query = f"""
                SELECT
                  t_id tradeid,
                  t_dts,
                  t_st_id,
                  t_tt_id,
                  t_is_cash,
                  t_s_symb,
                  t_qty AS quantity,
                  t_bid_price AS bidprice,
                  t_ca_id,
                  t_exec_name AS executedby,
                  t_trade_price AS tradeprice,
                  t_chrg AS fee,
                  t_comm AS commission,
                  t_tax AS tax,
                  t.batchid,
                  CASE 
                    WHEN cdc_flag = 'I' THEN True
                    WHEN t_st_id IN ('CMPT', 'CNCL') THEN False
                    ELSE cast(null as boolean) END AS create_flg
                FROM TPCDI_STG.BRONZE.TradeIncremental t
                WHERE batchid = cast({batch} as int)"""
    
    
            scd1_query = f"""select
            		* exclude(t_dts,
            		createdate),
            		nvl2(sk_createdateid, createdate, cast(null as timestamp)) createdate
            	from
            		(
            		select
            			tradeid,
            			min(date(t_dts)) over (partition by tradeid) createdate,
            			t_dts,
            			coalesce(sk_createdateid,
            			first_value(sk_createdateid) ignore nulls over ( partition by tradeid
            		order by
            			t_dts)) sk_createdateid,
            			coalesce(sk_createtimeid,
            			first_value(sk_createtimeid) ignore nulls over ( partition by tradeid
            		order by
            			t_dts)) sk_createtimeid,
            			coalesce(sk_closedateid,
            			last_value(sk_closedateid) ignore nulls over ( partition by tradeid
            		order by
            			t_dts)) sk_closedateid,
            			coalesce(sk_closetimeid,
            			last_value(sk_closetimeid) ignore nulls over ( partition by tradeid
            		order by
            			t_dts)) sk_closetimeid,
            			cashflag,
            			t_st_id,
            			t_tt_id,
            			t_s_symb,
            			quantity,
            			bidprice,
            			t_ca_id,
            			executedby,
            			tradeprice,
            			fee,
            			commission,
            			tax,
            			batchid
            		from
            			(
            			select
            				tradeid,
            				t_dts,
            				iff(create_flg,
            				sk_dateid,
            				cast(null as BIGINT)) sk_createdateid,
            				iff(create_flg,
            				sk_timeid,
            				cast(null as BIGINT)) sk_createtimeid,
            				iff(not create_flg,
            				sk_dateid,
            				cast(null as BIGINT)) sk_closedateid,
            				iff(not create_flg,
            				sk_timeid,
            				cast(null as BIGINT)) sk_closetimeid,
            				case
            					when t_is_cash = 1 then true
            					when t_is_cash = 0 then false
            					else cast(null as BOOLEAN)
            				end as cashflag,
            				t_st_id,
            				t_tt_id,
            				t_s_symb,
            				quantity,
            				bidprice,
            				t_ca_id,
            				executedby,
            				tradeprice,
            				fee,
            				commission,
            				tax,
            				t.batchid
            			from
            				({trade_query}) t
            			join TPCDI_WH.BRONZE.DimDate dd on
            				date(t.t_dts) = dd.datevalue
            			join TPCDI_WH.BRONZE.DimTime dt on
            				TO_TIME(t_dts) = dt.timevalue ) ) QUALIFY row_number() over (partition by tradeid
            	order by
            		t_dts desc) = 1"""
    
    
            stg_query = f"""select
            	trade.tradeid,
            	sk_brokerid,
            	trade.sk_createdateid,
            	trade.sk_createtimeid,
            	trade.sk_closedateid,
            	trade.sk_closetimeid,
            	st_name status,
            	tt_name type,
            	trade.cashflag,
            	sk_securityid,
            	sk_companyid,
            	trade.quantity,
            	trade.bidprice,
            	sk_customerid,
            	sk_accountid,
            	trade.executedby,
            	trade.tradeprice,
            	trade.fee,
            	trade.commission,
            	trade.tax,
            	trade.batchid
            from
            	({scd1_query}) trade
            join TPCDI_WH.BRONZE.StatusType status on
            	status.st_id = trade.t_st_id
            join TPCDI_WH.BRONZE.TradeType tt on
            	tt.tt_id = trade.t_tt_id
            left join TPCDI_WH.SILVER.DimSecurity ds on
            	ds.symbol = trade.t_s_symb
            	and createdate >= ds.effectivedate
            	and createdate < ds.enddate
            left join TPCDI_WH.SILVER.DimAccount da on
            	trade.t_ca_id = da.accountid
            	and createdate >= da.effectivedate
            	and createdate < da.enddate"""
            
            session.sql(f"""MERGE INTO TPCDI_WH.SILVER.DimTrade t
                  USING ({stg_query}) s
                    ON t.tradeid = s.tradeid
                  WHEN MATCHED THEN UPDATE SET
                      sk_closedateid = s.sk_closedateid,
                      sk_closetimeid = s.sk_closetimeid,
                      status = s.status,
                      type = s.type,
                      cashflag = s.cashflag,
                      quantity = s.quantity,
                      bidprice = s.bidprice,
                      executedby = s.executedby,
                      tradeprice = s.tradeprice,
                      fee = s.fee,
                      commission = s.commission,
                      tax = s.tax,
                      batchid = s.batchid
                  WHEN NOT MATCHED THEN INSERT ({tgt_cols})
                  VALUES ({tgt_cols})""").collect()
            
            
        ##----------------------------------------------------------------------------------------------------------------------------- 
    
        
        ##----------------------------------------------------------------------------------------------------------------------------- 
        ## FactHoldings
        
        if batch == '1':
            session.sql("""select distinct * from TPCDI_STG.BRONZE.HOLDING_HISTORY""") \
            .createOrReplaceTempView('TPCDI_STG.SILVER.HOLDINGHISTORYTEMP')
        else:
            session.sql(f"""select distinct hh_h_t_id,hh_t_id,hh_before_qty,hh_after_qty,batchid from TPCDI_STG.BRONZE.HOLDINGINCREMENTAL WHERE batchid = cast({batch} as int)  """) \
            .createOrReplaceTempView('TPCDI_STG.SILVER.HOLDINGHISTORYTEMP')
        
    
        session.sql(f"""  SELECT hh_h_t_id tradeid,  hh_t_id currenttradeid,  sk_customerid,  sk_accountid,  sk_securityid,  sk_companyid,  
        SK_CloseDateID sk_dateid,  sk_closetimeid sk_timeid,  TradePrice currentprice,  hh_after_qty currentholding,  {batch} batchid 
        FROM TPCDI_STG.SILVER.HOLDINGHISTORYTEMP hh  
        JOIN TPCDI_WH.SILVER.DIMTRADE dt  ON  dt.tradeid = hh.hh_t_id""") \
        .write.mode("append").save_as_table('TPCDI_WH.GOLD.FACTHOLDINGS')  
    
        ##-----------------------------------------------------------------------------------------------------------------------------      
        
    
                
        ##----------------------------------------------------------------------------------------------------------------------------- 
        ## FactMarketHistory    
        session.sql("SELECT sk_dateid, min(dm_low) OVER ( PARTITION BY dm_s_symb ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND 0 FOLLOWING ) fiftytwoweeklow, max(dm_high) OVER ( PARTITION by dm_s_symb ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND 0 FOLLOWING ) fiftytwoweekhigh, dmh.* FROM ( SELECT * FROM TPCDI_STG.BRONZE.V_DMH UNION ALL SELECT * exclude(cdc_flag, cdc_dsn) FROM TPCDI_STG.BRONZE.dailymarketincremental) dmh JOIN TPCDI_WH.BRONZE.DimDate d ON d.datevalue = dm_date") \
        .createOrReplaceTempView("TPCDI_STG.SILVER.tempDailyMarket")
    
        session.sql("SELECT * FROM ( SELECT a.*, b.sk_dateid AS sk_fiftytwoweeklowdate, c.sk_dateid AS sk_fiftytwoweekhighdate FROM TPCDI_STG.SILVER.tempDailyMarket a JOIN TPCDI_STG.SILVER.tempDailyMarket b ON a.dm_s_symb = b.dm_s_symb AND a.fiftytwoweeklow = b.dm_low AND b.dm_date between add_months(a.dm_date, -12) AND a.dm_date JOIN TPCDI_STG.SILVER.tempDailyMarket c ON a.dm_s_symb = c.dm_s_symb AND a.fiftytwoweekhigh = c.dm_high AND c.dm_date between add_months(a.dm_date, -12) AND a.dm_date ) dmh QUALIFY ROW_NUMBER() OVER ( PARTITION BY dm_s_symb, dm_date ORDER BY sk_fiftytwoweeklowdate, sk_fiftytwoweekhighdate) = 1") \
        .createOrReplaceTempView("TPCDI_STG.SILVER.DailyMarketStg")
        
        session.sql("SELECT sk_companyid, fi_qtr_start_date, sum(fi_basic_eps) OVER (PARTITION BY companyid ORDER BY fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - fi_basic_eps sum_fi_basic_eps FROM TPCDI_WH.SILVER.Financial JOIN TPCDI_WH.SILVER.DimCompany USING (sk_companyid)") \
        .createOrReplaceTempView("TPCDI_STG.SILVER.CompanyFinancialsStg")
        
        if batch == '1':
            session.sql(f"SELECT s.sk_securityid, s.sk_companyid, sk_dateid, iff(sum_fi_basic_eps>0,dmh.dm_close / sum_fi_basic_eps,null) AS peratio, (s.dividend / dmh.dm_close) / 100 AS yield, fiftytwoweekhigh, sk_fiftytwoweekhighdate, fiftytwoweeklow, sk_fiftytwoweeklowdate, dm_close closeprice, dm_high dayhigh, dm_low daylow, dm_vol volume, dmh.batchid FROM TPCDI_STG.SILVER.DailyMarketStg dmh JOIN TPCDI_WH.SILVER.DimSecurity s ON s.symbol = dmh.dm_s_symb and dmh.dm_date >= s.effectivedate AND dmh.dm_date < s.enddate LEFT JOIN TPCDI_STG.SILVER.CompanyFinancialsStg f ON f.sk_companyid = s.sk_companyid AND quarter(dmh.dm_date) = quarter(fi_qtr_start_date) AND year(dmh.dm_date) = year(fi_qtr_start_date) WHERE dmh.batchid = {batch}") \
            .write.mode("append").save_as_table('TPCDI_WH.GOLD.FactMarketHistory')
        else:
            session.sql(f"SELECT s.sk_securityid, s.sk_companyid, sk_dateid, iff(sum_fi_basic_eps>0,dmh.dm_close / sum_fi_basic_eps,null) AS peratio, (s.dividend / dmh.dm_close) / 100 AS yield, fiftytwoweekhigh, sk_fiftytwoweekhighdate, fiftytwoweeklow, sk_fiftytwoweeklowdate, dm_close closeprice, dm_high dayhigh, dm_low daylow, dm_vol volume, dmh.batchid FROM TPCDI_STG.SILVER.DailyMarketStg dmh JOIN TPCDI_WH.SILVER.DimSecurity s ON s.symbol = dmh.dm_s_symb and s.iscurrent LEFT JOIN TPCDI_STG.SILVER.CompanyFinancialsStg f ON f.sk_companyid = s.sk_companyid AND quarter(dmh.dm_date) = quarter(fi_qtr_start_date) AND year(dmh.dm_date) = year(fi_qtr_start_date) WHERE dmh.batchid = {batch}") \
            .write.mode("append").save_as_table('TPCDI_WH.GOLD.FactMarketHistory')
                
        ##----------------------------------------------------------------------------------------------------------------------------- 
        
    
        ###==============================================================================================================================
        ### FactWatches Start
        ###==============================================================================================================================        
    
        watch_tgt_cols = "sk_customerid, sk_securityid, sk_dateid_dateplaced, sk_dateid_dateremoved, batchid"
        
        if batch == '1':
            session.sql("select * from TPCDI_STG.BRONZE.WATCH_HISTORY") \
            .createOrReplaceTempView('TPCDI_STG.PUBLIC.watch_hist_temp')
        else:
            session.sql(f""" SELECT DISTINCT w_c_id,w_s_symb,w_DTS,w_action,batchid FROM TPCDI_STG.BRONZE.WATCHINCREMENTAL WHERE batchid = cast({batch} as int)""") \
            .createOrReplaceTempView('TPCDI_STG.PUBLIC.watch_hist_temp')
        
        session.sql("""SELECT  w_c_id customerid,  w_s_symb symbol,  d.SK_DATEID,  d.DATEVALUE recorddate,  w_action FROM TPCDI_STG.PUBLIC.watch_hist_temp JOIN TPCDI_WH.BRONZE.DIMDATE d ON d.DATEVALUE = date(w_dts)""") \
        .createOrReplaceTempView('Watch_date')
        
        ###=============================================================================================================================
        ### Need to keep the orig values of the ACTV record, only need to change dateremoved on CNCL record
        ###=============================================================================================================================
        
        session.sql(""" SELECT DISTINCT
            nvl(actv.customerid, cncl.customerid) customerid,
            nvl(actv.symbol, cncl.symbol) symbol,
            actv.recorddate,
            SUBSTR(TO_CHAR(actv.SK_DATEID), 1, 4) || SUBSTR(TO_CHAR(actv.SK_DATEID), 5, 2) || SUBSTR(TO_CHAR(actv.SK_DATEID), 7, 2) AS sk_dateid_dateplaced, 
            SUBSTR(TO_CHAR(cncl.SK_DATEID), 1, 4) || SUBSTR(TO_CHAR(cncl.SK_DATEID), 5, 2) || SUBSTR(TO_CHAR(cncl.SK_DATEID), 7, 2) AS sk_dateid_dateremoved
            FROM (SELECT * FROM Watch_date WHERE w_action = 'ACTV') actv
            FULL OUTER JOIN (SELECT * FROM Watch_date WHERE w_action = 'CNCL') cncl 
            ON actv.customerid = cncl.customerid  AND actv.symbol = cncl.symbol """) \
        .createOrReplaceTempView('Watch_tmp')
        
        ###=============================================================================================================================
        ### Historical needs to handle effective/end dates. Otherwise just need currently active record
        ###=============================================================================================================================
        
        ##-- replace 1 with batch id
        ##-- need to replace insertedts with effectivedate/enddate
        ##-- This limits these results to only the records in the batch where at least there was a watch created in the batch, otherwise if it is null then it is a CANCEL record removing a past batch's watch
        
        if batch == '1':
            session.sql(f""" 
                SELECT DISTINCT
                c.SK_CUSTOMERID sk_customerid,
                s.sk_securityid sk_securityid,
                w.sk_dateid_dateplaced,
                w.sk_dateid_dateremoved,
                cast({batch} as int) batchid 
                FROM Watch_tmp w
                JOIN TPCDI_STG.SILVER.dimcustomerstg c ON w.customerid = c.customerid AND w.recorddate >= c.effectivedate AND w.recorddate < c.enddate
                JOIN TPCDI_WH.SILVER.DimSecurity s  ON s.symbol = w.symbol AND w.recorddate >= s.effectivedate AND w.recorddate < s.enddate 
                WHERE w.sk_dateid_dateplaced IS NOT NULL
                """) \
            .createOrReplaceTempView('Watch_actv')
            
        else:
            session.sql(f""" 
                SELECT DISTINCT
                c.SK_CUSTOMERID sk_customerid,
                s.sk_securityid sk_securityid,
                w.sk_dateid_dateplaced,
                w.sk_dateid_dateremoved,
                cast({batch} as int) batchid 
                FROM Watch_tmp w
                JOIN TPCDI_STG.SILVER.dimcustomerstg c ON w.customerid = c.customerid AND c.iscurrent  
                JOIN TPCDI_WH.SILVER.DimSecurity s  ON s.symbol = w.symbol AND s.iscurrent
                WHERE w.sk_dateid_dateplaced IS NOT NULL
                """) \
            .createOrReplaceTempView('Watch_actv')
        
        ##-- replace 1 with batch id variable
        ##-- Active watches
        ## ---- Canceled Watches
        session.sql(f"""
            SELECT DISTINCT
            fw.sk_customerid, 
            fw.sk_securityid, 
            w.sk_dateid_dateplaced, 
            w.sk_dateid_dateremoved,
            cast({batch} as int) batchid  
            FROM (
            SELECT  sk_customerid,  sk_securityid  FROM TPCDI_WH.GOLD.FACTWATCHES  WHERE sk_dateid_dateremoved is null 
            ) fw
            JOIN TPCDI_STG.SILVER.dimcustomerstg c ON fw.sk_customerid = c.SK_CUSTOMERID
            JOIN TPCDI_WH.SILVER.DimSecurity s ON fw.sk_securityid = s.sk_securityid
            JOIN (  SELECT customerid,  symbol,  sk_dateid_dateplaced,  sk_dateid_dateremoved  FROM Watch_tmp  WHERE sk_dateid_dateplaced is null ) w 
            ON  w.customerid = c.customerid  AND  w.symbol = s.symbol
            """) \
        .createOrReplaceTempView('Watch_cncl')
        
        if batch == '1':
            session.sql(""" SELECT * FROM Watch_actv  """) \
            .write.mode("append").saveAsTable('TPCDI_WH.GOLD.FACTWATCHES')
            
        else:
            session.sql(f"""
            MERGE INTO TPCDI_WH.GOLD.FACTWATCHES t
            USING (
            SELECT
            sk_customerid AS merge_sk_customerid,
            sk_securityid AS merge_sk_securityid,
            wc.*
            FROM Watch_cncl wc
            UNION ALL
            SELECT 
            CAST(NULL AS BIGINT) AS merge_sk_customerid,
            CAST(NULL AS BIGINT) AS merge_sk_securityid,
            wa.*
            FROM Watch_actv wa
            WHERE sk_dateid_dateplaced IS NOT NULL) s
            ON 
            t.sk_customerid = s.merge_sk_customerid
            AND t.sk_securityid = s.merge_sk_securityid
            AND t.sk_dateid_dateremoved is null
            WHEN MATCHED THEN UPDATE SET
            t.sk_dateid_dateremoved = s.sk_dateid_dateremoved
            ,t.batchid = s.batchid
            WHEN NOT MATCHED THEN 
            INSERT ({watch_tgt_cols})
            VALUES ({watch_tgt_cols})
            """).collect()
        
        
        ###==============================================================================================================================
        ###batch_validation
        batch_validation(batch)
                
        ###==============================================================================================================================
        ###FactWatches End
        ###============================================================================================================================== 
        
        

    # Auto audit validation
    ###==============================================================================================================================
    auto_audit()
        
    ##----------------------------------------------------------------------------------------------------------------------------- 
    ##Testing here
    # tableName = 'DimBroker'
    # dataframe = session.table(tableName)
    
    # # Print a sample of the dataframe to standard output.
    # dataframe.show()
    
    ##-----------------------------------------------------------------------------------------------------------------------------
    
    # Return value will appear in the Results tab.
    return "Execution completed"