CREATE PROCEDURE EXT.TRYG_SH_CLAWBACK ( in_PeriodSeq BIGINT,in_ProcessingUnitSeq BIGINT) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
DEFAULT SCHEMA EXT AS 
/*---------------------------------------------------------------------
    | Author: Sharath K
    | Project Title: Consultant
    | Company: SAP Callidus
    | Initial Version Date: 19-April-2022
    |----------------------------------------------------------------------
    | Procedure Purpose: 
    | Version: 0.1	19-April-2022	Intial Version
    -----------------------------------------------------------------------
    */
BEGIN
	--Row type variables declarations
	DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
	DECLARE v_puRow ROW LIKE TCMP.CS_PROCESSINGUNIT;
	DECLARE v_unitTypeRow ROW LIKE TCMP.CS_UNITTYPE;


	--Variable declarations
	DECLARE v_tenantid VARCHAR(50);
	DECLARE v_procedureName VARCHAR(50);
	DECLARE v_slqerrm VARCHAR(4000);
	DECLARE v_eventType VARCHAR(50);

	DECLARE v_removeDate DATE;
	DECLARE v_executionDate TIMESTAMP;
	DECLARE v_lastrunDate TIMESTAMP;

	DECLARE v_Count INT;
	DECLARE v_sqlCount INT;


	-- Exeception Handling
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN v_slqerrm := ::SQL_ERROR_MESSAGE;
		CALL EXT.TRYG_LOG(v_procedureName,'ERROR = '||IFNULL(:v_slqerrm,'') ,NULL);
	END;

	--------------------------------------------------------------------------- 
	v_procedureName = 'TRYG_SH_CLAWBACK';
	-- v_eventType = 'SC-DK-001-001';
	v_eventType = 'SC-DK-001-001-SUMMARY';
	v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');
	v_executionDate	= current_timestamp;
	v_sqlCount = 0;
	v_Count = 0;

	SELECT * INTO v_puRow FROM TCMP.CS_PROCESSINGUNIT cp WHERE cp.PROCESSINGUNITSEQ = in_ProcessingUnitSeq;
	SELECT * INTO v_periodRow FROM TCMP.CS_PERIOD cp WHERE cp.PERIODSEQ = in_PeriodSeq AND cp.REMOVEDATE = v_removeDate;
	SELECT * INTO v_unitTypeRow FROM TCMP.CS_UNITTYPE cu WHERE cu.REMOVEDATE = v_removeDate AND cu.name = 'quantity';

	v_tenantid = :v_puRow.TENANTID;


	EXT.TRYG_LOG(v_procedureName,'####   BEGIN   #### '||:v_periodRow.Name,NULL);

	SELECT
		ifnull(max(executionDate), to_timestamp('01/01/1900 00:00:00', 'dd/mm/yyyy HH24:MI:SS'))
		INTO
		v_lastrundate
	FROM
		ext.TRYG_SH_CLAWBACK_LKTB;

	CALL EXT.TRYG_LOG(v_procedureName,'last execution date for lookup table  = '|| v_lastrundate,NULL);
	COMMIT;

	SELECT
		count(*)
		INTO
		v_count
	FROM
		cs_relationalmdlt mdlt
	WHERE
		mdlt.name = 'LT_Agent_Type_Eligible_Clawback'
		AND mdlt.removedate = v_removedate
		AND mdlt.createdate > v_lastrundate;

	CALL EXT.TRYG_LOG(v_procedureName,'count check for if LT is changed = '|| v_count,NULL);
	COMMIT;

	IF v_count > 0
	THEN
		DELETE FROM ext.tryg_sh_clawback_lktb;
		v_sqlCount = ::ROWCOUNT;
		CALL EXT.TRYG_LOG(v_procedureName,'Deleting existing values to insert modified cell value Complete',v_sqlCount);
		COMMIT;
		
		INSERT INTO ext.tryg_sh_clawback_lktb
		(
			SELECT
				v_tenantid,
				mdlt.name,
				dim1.name AS dim_Name,
				ind1.minstring dim_indices,
				cell.VALUE,
				mdlt.createdate,
				v_executionDate AS executiondate
			FROM
				cs_relationalmdlt mdlt
			INNER JOIN cs_mdltdimension dim1 ON
				dim1.ruleelementseq = mdlt.ruleelementseq
				AND dim1.dimensionseq = 1
				AND dim1.removedate = v_removedate
				AND dim1.modelseq = 0
			INNER JOIN cs_mdltindex ind1 ON
				ind1.ruleelementseq = mdlt.ruleelementseq
				AND ind1.dimensionseq = dim1.dimensionseq
				AND ind1.removedate = v_removedate
				AND ind1.modelseq = 0
			LEFT JOIN cs_mdltcell cell ON
				cell.mdltseq = MDLT.RULEELEMENTSEQ
				AND cell.removedate = v_removedate
				AND cell.modelseq = 0
				AND dim0index = ind1.ordinal
			WHERE
				mdlt.removedate = v_removedate
				AND mdlt.modelseq = 0 
				AND mdlt.name LIKE 'LT_Agent_Type_Eligible_Clawback'
		);
		v_sqlCount = ::ROWCOUNT;
		CALL EXT.TRYG_LOG(v_procedureName,'Inserting eligible title lookup table values into tryg_sh_clawback_lktb Complete',v_sqlCount);
		COMMIT;
		
	END IF;


	SELECT
		ifnull(max(executionDate), to_timestamp('01/01/1900 00:00:00', 'dd/mm/yyyy HH24:MI:SS'))
		INTO
		v_lastrundate
	FROM
		ext.tryg_sh_clawback_Txns
	WHERE
		tenantid = v_tenantid
		AND processingunitseq = in_processingunitseq;

	CALL EXT.TRYG_LOG(v_procedureName,'last execution date for transaction table  = '|| v_lastrundate,NULL);
	COMMIT;

--#######################################################################################################################################
--#######################################################################################################################################
	UPDATE cs_salestransaction st
	SET genericattribute10 = 'DECR'
	WHERE
		st.compensationdate >= :v_periodRow.startDate
		AND st.compensationdate < :v_periodRow.enddate
		AND st.genericdate3 IS NULL
		AND st.genericnumber1 > st.genericnumber2
		AND st.eventtypeseq IN (SELECT DATAtypeseq FROM cs_eventtype WHERE removedate = v_removedate AND eventtypeid = v_eventType)
		AND st.genericattribute10 IS NULL 
		AND EXISTS (
		SELECT
			st_in.salestransactionseq AS cantxns_salestransactionseq,
			st_in.linenumber AS cantxns_linenumber,
			st_in.sublinenumber AS cantxns_sublinenumber,
			st_in.alternateordernumber AS cantxns_alternateordernumber,
			st_in.genericnumber1 AS cantxns_Old_premium,
			st_in.genericnumber2 AS cantxns_new_premium,
			st_in.genericdate1 AS cantxns_policy_sDate,
			st_in.genericdate2 AS cantxns_policy_eDate,
			st_in.genericdate3 AS cantxns_policy_cDate,
			st_in.*
		FROM
			cs_salestransaction st_in
		WHERE
			st_in.compensationdate >= :v_periodRow.startDate
			AND st_in.compensationdate < :v_periodRow.enddate
			AND st_in.genericdate3 IS NULL
			AND st_in.genericnumber1 < st_in.genericnumber2
			AND st_in.eventtypeseq IN (SELECT DATAtypeseq FROM cs_eventtype WHERE removedate = v_removedate AND eventtypeid = v_eventType)
			AND st_in.alternateordernumber = st.alternateordernumber 
			AND IFNULL(st_in.genericdate1,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(st.genericdate1,to_date('01/01/2000','mm/dd/yyyy'))
			AND IFNULL(st_in.genericdate2,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(st.genericdate2,to_date('01/01/2000','mm/dd/yyyy'))

		);

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericattribute10 for DECR txns having new txns in same month',v_sqlCount);	
	COMMIT;

	UPDATE cs_salestransaction st
	SET genericattribute10 = 'CANC'
	WHERE
		st.compensationdate >= :v_periodRow.startDate
		AND st.compensationdate < :v_periodRow.enddate
		AND st.genericdate3 IS NULL
		AND st.genericnumber1 > st.genericnumber2
		AND st.eventtypeseq IN (SELECT DATAtypeseq FROM cs_eventtype WHERE removedate = v_removedate AND eventtypeid = v_eventType)
		AND st.genericattribute10 IS NULL 
		AND EXISTS (
		SELECT
			st_in.salestransactionseq AS cantxns_salestransactionseq,
			st_in.linenumber AS cantxns_linenumber,
			st_in.sublinenumber AS cantxns_sublinenumber,
			st_in.alternateordernumber AS cantxns_alternateordernumber,
			st_in.genericnumber1 AS cantxns_Old_premium,
			st_in.genericnumber2 AS cantxns_new_premium,
			st_in.genericdate1 AS cantxns_policy_sDate,
			st_in.genericdate2 AS cantxns_policy_eDate,
			st_in.genericdate3 AS cantxns_policy_cDate,
			st_in.*
		FROM
			cs_salestransaction st_in
		WHERE
			st_in.compensationdate >= :v_periodRow.startDate
			AND st_in.compensationdate < :v_periodRow.enddate
			AND st_in.genericdate3 IS NOT NULL
			AND st_in.genericnumber1 < st_in.genericnumber2
			AND st_in.eventtypeseq IN (SELECT DATAtypeseq FROM cs_eventtype WHERE removedate = v_removedate AND eventtypeid = v_eventType)
			AND st_in.alternateordernumber = st.alternateordernumber 
			AND IFNULL(st_in.genericdate1,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(st.genericdate1,to_date('01/01/2000','mm/dd/yyyy'))
			AND IFNULL(st_in.genericdate2,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(st.genericdate2,to_date('01/01/2000','mm/dd/yyyy'))

		);

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericattribute10 for CANC txns having new txns in same month',v_sqlCount);	
	COMMIT;


-- UPDATE cs_salestransaction st set st.genericattribute10 =(select genericattribute10 from cs_salestransaction st_in
--  where 	st_in.compensationdate >= :v_periodRow.startDate
--  AND st_in.compensationdate < :v_periodRow.enddate
--  and st_in.genericattribute10 in ('CANC','DECR')
--  AND st_in.genericdate3 IS NOT NULL
--  and st.alternateordernumber=st_in.alternateordernumber
--  and st.sublinenumber=st_in.sublinenumber
--  and st.compensationdate=st_in.compensationdate
--  and st.linenumber=st_in.linenumber)
--  where st.eventtypeseq =(select datatypeseq from cs_eventtype where eventtypeid='SC-DK-001-001-SUMMARY' and removedate=:v_removeDate ) 
--  AND st.compensationdate >= :v_periodRow.startDate
--  AND st.compensationdate < :v_periodRow.enddate;


	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericattribute10 for SUMM txns having new txns in same month',v_sqlCount);	
	COMMIT;



--#######################################################################################################################################
--#######################################################################################################################################
	MERGE
	INTO
		cs_transactionassignment tgt
		USING
		(
			SELECT * FROM 
			(
				SELECT
					st.salestransactionseq AS cantxns_salestransactionseq,
					st.salesorderseq AS cantxns_salesorderseq,
					st.linenumber AS cantxns_linenumber,
					st.sublinenumber AS cantxns_sublinenumber,
					st.alternateordernumber AS cantxns_alternateordernumber,
					st.compensationdate AS cantxns_compdate,
					st.genericnumber1 AS cantxns_Old_premium,
					st.genericnumber2 AS cantxns_new_premium,
					st.genericdate1 AS cantxns_policy_sDate,
					st.genericdate2 AS cantxns_policy_eDate,
					st.genericdate3 AS cantxns_policy_cDate,
					sta.positionname AS cantxns_positionname,
					(
						SELECT st_sub.genericnumber2 FROM (SELECT  MAX(st_lp.sublinenumber) as sublinenumber, st_lp.alternateordernumber, st_lp.compensationdate 
						FROM cs_salestransaction st_lp 	
						INNER JOIN cs_transactionassignment sta_lp ON
							sta_lp.salestransactionseq = st_lp.salestransactionseq
							AND sta_lp.compensationdate = st_lp.compensationdate
						INNER JOIN cs_eventtype et_lp ON
							et_lp.datatypeseq = st_lp.eventtypeseq
							AND et_lp.removedate = :v_removedate  
						WHERE 
							st_lp.alternateordernumber = st.alternateordernumber
                            AND sta_lp.processingunitseq= in_ProcessingUnitSeq
							AND sta_lp.positionname = sta.positionname 
							AND et_lp.eventtypeid = :v_eventType
							AND st_lp.compensationdate < st.compensationdate
							AND st_lp.compensationdate = (   
										SELECT max(st_lp_in.compensationdate) 
										FROM cs_salestransaction st_lp_in 	
										INNER JOIN cs_transactionassignment sta_lp_in ON
											sta_lp_in.salestransactionseq = st_lp_in.salestransactionseq
											AND sta_lp_in.compensationdate = st_lp_in.compensationdate
										INNER JOIN cs_eventtype et_lp_in ON
											et_lp_in.datatypeseq = st_lp_in.eventtypeseq
											AND et_lp_in.removedate = :v_removedate  
										WHERE 
											st_lp_in.alternateordernumber = st.alternateordernumber
                                            AND sta_lp_in.processingunitseq= in_ProcessingUnitSeq
											AND sta_lp_in.positionname = sta_lp.positionname
											AND et_lp_in.eventtypeid = :v_eventType
											AND st_lp_in.compensationdate < st.compensationdate
											) GROUP BY st_lp.alternateordernumber, st_lp.compensationdate) MAX_SUB
                                            INNER JOIN CS_SALESTRANSACTION st_sub on
										 st_sub.compensationdate=A.compensationdate 
										 AND MAX_SUB.alternateordernumber = st_sub.alternateordernumber
										 AND MAX_SUB.sublinenumber=st_sub.sublinenumber
										 INNER JOIN cs_eventtype et_sub ON
											et_sub.datatypeseq = st_sub.eventtypeseq
											AND et_sub.removedate = :v_removedate 
											where et_sub.eventtypeid = :v_eventType) lastest_premium
				FROM
					cs_salestransaction st
				INNER JOIN cs_transactionassignment sta ON
					sta.salestransactionseq = st.salestransactionseq
					AND sta.compensationdate = st.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st.eventtypeseq
					AND et.removedate = v_removedate
				WHERE
					st.genericdate3 IS NULL
                    AND sta.processingunitseq= in_ProcessingUnitSeq
					AND st.genericnumber1 > st.genericnumber2  -- Old Premium Less than NEW Premium FOR NEW AND Increase txns
					AND et.eventtypeid = v_eventType
					AND st.compensationdate >= :v_periodRow.startDate
					AND st.compensationdate < :v_periodRow.endDate
--					AND st.genericattribute10 IS null
--					AND alternateordernumber = '6055002703399'
			) cantxns
			LEFT JOIN 
			(
				SELECT 	st.alternateordernumber AS newtxns_alternateordernumber,
					sta.positionname AS newtxns_positionname,
					st.genericdate1 AS newtxns_policy_sDate,
					st.genericdate2 AS newtxns_policy_eDate,
					sum(IFNULL(cc.value,0)) AS newtxns_crdvalue
				FROM
					cs_salestransaction st
				INNER JOIN cs_transactionassignment sta ON
					sta.salestransactionseq = st.salestransactionseq
					AND sta.compensationdate = st.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st.eventtypeseq
					AND et.removedate = v_removedate
				LEFT JOIN cs_credit cc ON
					cc.salestransactionseq = st.salestransactionseq
				LEFT JOIN cs_position pos ON	
					pos.name = sta.positionname
					AND pos.removedate = v_removedate
					AND pos.effectiveenddate > current_date
					AND pos.effectivestartdate <= current_date
					AND pos.ruleelementownerseq = cc.positionseq
				WHERE
					st.genericdate3 IS NULL
                    AND sta.processingunitseq= in_ProcessingUnitSeq
					AND et.eventtypeid = v_eventType
					AND st.compensationdate >= add_months(:v_periodRow.startDate,-12)
					AND st.compensationdate < :v_periodRow.startDate
					AND IFNULL(cc.periodseq, :v_periodRow.periodseq) <= :v_periodRow.periodseq
--					AND alternateordernumber = '6055002703399'	
				GROUP BY 
					st.alternateordernumber ,
					sta.positionname ,
					st.genericdate1 ,
					st.genericdate2
			) newtxns ON 
			cantxns_alternateordernumber = newtxns_alternateordernumber
			AND cantxns_positionname = newtxns_positionname
			AND IFNULL(newtxns_policy_sDate,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(cantxns.cantxns_policy_sDate,to_date('01/01/2000','mm/dd/yyyy'))
			AND IFNULL(newtxns_policy_eDate,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(cantxns.cantxns_policy_eDate,to_date('01/01/2000','mm/dd/yyyy'))
		) src ON 
		(
			src.cantxns_salestransactionseq = tgt.SALESTRANSACTIONSEQ
			AND src.cantxns_positionname = tgt.positionname
		)
		WHEN MATCHED THEN
		UPDATE SET 
			tgt.genericnumber2 = src.newtxns_crdvalue,
			tgt.unittypeforgenericnumber2 = :v_unitTypeRow.unittypeseq ,
			tgt.genericnumber3 = src.lastest_premium,
			tgt.unittypeforgenericnumber3 = :v_unitTypeRow.unittypeseq 
		;

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericnumber2 for Decrease txns with credit value',v_sqlCount);	
	COMMIT;



--#######################################################################################################################################
--#######################################################################################################################################
	MERGE
	INTO
		cs_transactionassignment tgt
		USING
		(
			SELECT * FROM 
			(
				SELECT
					st.salestransactionseq AS cantxns_salestransactionseq,
					st.salesorderseq AS cantxns_salesorderseq,
					st.linenumber AS cantxns_linenumber,
					st.sublinenumber AS cantxns_sublinenumber,
					st.alternateordernumber AS cantxns_alternateordernumber,
					st.compensationdate AS cantxns_compdate,
					st.genericnumber1 AS cantxns_Old_premium,
					st.genericnumber2 AS cantxns_new_premium,
					st.genericdate1 AS cantxns_policy_sDate,
					st.genericdate2 AS cantxns_policy_eDate,
					st.genericdate3 AS cantxns_policy_cDate,
					sta.positionname AS cantxns_positionname,
					(
						SELECT st_lp.genericnumber2 
						FROM cs_salestransaction st_lp 	
						INNER JOIN cs_transactionassignment sta_lp ON
							sta_lp.salestransactionseq = st_lp.salestransactionseq
							AND sta_lp.compensationdate = st_lp.compensationdate
						INNER JOIN cs_eventtype et_lp ON
							et_lp.datatypeseq = st_lp.eventtypeseq
							AND et_lp.removedate = :v_removedate  
						WHERE 
							st_lp.alternateordernumber = st.alternateordernumber
                            AND sta_lp.processingunitseq= in_ProcessingUnitSeq
							AND sta_lp.positionname = sta.positionname 
							AND et_lp.eventtypeid = :v_eventType
							AND st_lp.compensationdate < st.compensationdate
							AND st_lp.compensationdate = (   
										SELECT max(st_lp_in.compensationdate) 
										FROM cs_salestransaction st_lp_in 	
										INNER JOIN cs_transactionassignment sta_lp_in ON
											sta_lp_in.salestransactionseq = st_lp_in.salestransactionseq
											AND sta_lp_in.compensationdate = st_lp_in.compensationdate
										INNER JOIN cs_eventtype et_lp_in ON
											et_lp_in.datatypeseq = st_lp_in.eventtypeseq
											AND et_lp_in.removedate = :v_removedate  
										WHERE 
											st_lp_in.alternateordernumber = st.alternateordernumber
                                            AND sta_lp_in.processingunitseq= in_ProcessingUnitSeq
											AND sta_lp_in.positionname = sta_lp.positionname
											AND et_lp_in.eventtypeid = :v_eventType
											AND st_lp_in.compensationdate < st.compensationdate
											)) lastest_premium
				FROM
					cs_salestransaction st
				INNER JOIN cs_transactionassignment sta ON
					sta.salestransactionseq = st.salestransactionseq
					AND sta.compensationdate = st.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st.eventtypeseq
					AND et.removedate = v_removedate
				WHERE
					st.genericdate3 IS NOT NULL
                    AND sta.processingunitseq= in_ProcessingUnitSeq
					AND st.genericattribute1 = 'AFGA'
					AND st.genericnumber1 > st.genericnumber2  -- Old Premium Less than NEW Premium FOR NEW AND Increase txns
					AND et.eventtypeid = v_eventType
					AND st.compensationdate >= :v_periodRow.startDate
					AND st.compensationdate < :v_periodRow.endDate
--					AND st.genericattribute10 IS null
--					AND alternateordernumber = '6055002703399'
			) cantxns
			INNER JOIN 
			(
				SELECT 	st.alternateordernumber AS newtxns_alternateordernumber,
					sta.positionname AS newtxns_positionname,
					st.genericdate1 AS newtxns_policy_sDate,
					st.genericdate2 AS newtxns_policy_eDate,
					sum(cc.value) AS newtxns_crdvalue
				FROM
					cs_salestransaction st
				INNER JOIN cs_transactionassignment sta ON
					sta.salestransactionseq = st.salestransactionseq
					AND sta.compensationdate = st.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st.eventtypeseq
					AND et.removedate = v_removedate
				LEFT JOIN cs_credit cc ON
					cc.salestransactionseq = st.salestransactionseq
				LEFT JOIN cs_position pos ON	
					pos.name = sta.positionname
					AND pos.removedate = v_removedate
					AND pos.effectiveenddate > current_date
					AND pos.effectivestartdate <= current_date
					AND pos.ruleelementownerseq = cc.positionseq
				WHERE
					st.genericdate3 IS NULL
                    AND sta.processingunitseq= in_ProcessingUnitSeq
					AND et.eventtypeid = v_eventType
					AND st.compensationdate >= add_months(:v_periodRow.startDate,-12)
					AND st.compensationdate < :v_periodRow.startDate
					AND IFNULL(cc.periodseq, :v_periodRow.periodseq) <= :v_periodRow.periodseq
--					AND alternateordernumber = '6055002703399'	
				GROUP BY 
					st.alternateordernumber ,
					sta.positionname ,
					st.genericdate1 ,
					st.genericdate2
			) newtxns ON 
			cantxns_alternateordernumber = newtxns_alternateordernumber
			AND cantxns_positionname = newtxns_positionname
			AND IFNULL(newtxns_policy_sDate,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(cantxns.cantxns_policy_sDate,to_date('01/01/2000','mm/dd/yyyy'))
			AND IFNULL(newtxns_policy_eDate,to_date('01/01/2000','mm/dd/yyyy')) = IFNULL(cantxns.cantxns_policy_eDate,to_date('01/01/2000','mm/dd/yyyy'))
		) src ON 
		(
			src.cantxns_salestransactionseq = tgt.SALESTRANSACTIONSEQ
			AND src.cantxns_positionname = tgt.positionname
		)
		WHEN MATCHED THEN
		UPDATE SET 
			tgt.genericnumber2 = src.newtxns_crdvalue,
			tgt.unittypeforgenericnumber2 = :v_unitTypeRow.unittypeseq ,
			tgt.genericnumber3 = src.lastest_premium,
			tgt.unittypeforgenericnumber3 = :v_unitTypeRow.unittypeseq 
		;

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericnumber2 for Cancelled txns with credit value',v_sqlCount);	
	COMMIT;


-- UPDATE cs_transactionassignment ta set ta.genericnumber2 =(select genericnumber2 from cs_transactionassignment ta_in
--  where exists (select * from cs_salestransaction st_in
--  where st_in.salestransactionseq=ta_in.salestransactionseq
--  AND st_in.compensationdate >= :v_periodRow.startDate
--  AND st_in.compensationdate < :v_periodRow.enddate
--  AND st_in.genericdate3 IS NOT NULL
--  AND st_in.eventtypeseq =(select datatypeseq from cs_eventtype where eventtypeid='SC-DK-001-001-SUMMARY' and removedate=:v_removeDate ) 
--  )) where exists (select * from cs_salestransaction st_in
--  where st_in.salestransactionseq=ta.salestransactionseq
--  AND st_in.compensationdate >= :v_periodRow.startDate
--  AND st_in.compensationdate < :v_periodRow.enddate
--  AND st_in.genericdate3 IS NOT NULL
--  AND st_in.eventtypeseq =(select datatypeseq from cs_eventtype where eventtypeid='SC-DK-001-001-SUMMARY' and removedate=:v_removeDate ));
 
--  	v_sqlCount = ::ROWCOUNT;	
-- 	CALL EXT.TRYG_LOG(v_procedureName,'Updating the TA.genericnumber2 for SUMMARY txns with credit value',v_sqlCount);	
-- 	COMMIT;
                      
	COMMIT;
	EXT.TRYG_LOG(v_procedureName,'####   END   ####',NULL);

	
END