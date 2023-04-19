CREATE PROCEDURE EXT.TRYG_SH_POLICYPAY ( in_PeriodSeq BIGINT,in_ProcessingUnitSeq BIGINT) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
DEFAULT SCHEMA EXT AS 
/*---------------------------------------------------------------------
    | Author: Sharath K
    | Project Title: Consultant
    | Company: SAP Callidus
    | Initial Version Date: 01-May-2022
    |----------------------------------------------------------------------
    | Procedure Purpose: 
    | Version: 0.1	01-May-2022	Intial Version
    -----------------------------------------------------------------------
    */
BEGIN
	--Row type variables declarations
	DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
	DECLARE v_puRow ROW LIKE TCMP.CS_PROCESSINGUNIT;



	--Variable declarations
	DECLARE v_tenantid VARCHAR(50);
	DECLARE v_procedureName VARCHAR(50);
	DECLARE v_slqerrm VARCHAR(4000);
	DECLARE v_policyPay_ET VARCHAR(50);
	DECLARE v_policySales_ET VARCHAR(50);
	DECLARE v_policySalesSummary_ET VARCHAR(50);


	DECLARE v_sqlCount INT;

	DECLARE v_removeDate DATE;
	DECLARE v_changeDate DATE;


	-- Exeception Handling
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN v_slqerrm := ::SQL_ERROR_MESSAGE;
		CALL EXT.TRYG_LOG(v_procedureName,'ERROR = '||IFNULL(:v_slqerrm,'') ,NULL);
	END;

	--------------------------------------------------------------------------- 
	v_procedureName = 'TRYG_SH_POLICYPAY';
	v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');
	v_policySales_ET = 'SC-DK-001-001';
	v_policyPay_ET = 'SC-DK-001-002';
 	v_policySalesSummary_ET = 'SC-DK-001-001-SUMMARY';
 	v_changeDate = TO_DATE('01/01/2023','mm/dd/yyyy');


	SELECT * INTO v_puRow FROM TCMP.CS_PROCESSINGUNIT cp WHERE cp.PROCESSINGUNITSEQ = in_ProcessingUnitSeq;
	SELECT * INTO v_periodRow FROM TCMP.CS_PERIOD cp WHERE cp.PERIODSEQ = in_PeriodSeq AND cp.REMOVEDATE = v_removeDate;

	v_tenantid = :v_puRow.TENANTID;

	EXT.TRYG_LOG(v_procedureName,'####   BEGIN   #### '||:v_periodRow.Name,NULL);

	DELETE FROM ext.TRYG_SH_SALESTXNS_POLICYCRDS WHERE periodseq = :v_periodRow.periodseq;
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Deleting Previously inserted Policy Payment transactions for the month from temp table',v_sqlCount);
	COMMIT;


	INSERT INTO	ext.TRYG_SH_SALESTXNS_POLICYCRDS
	(
		SELECT
			so.orderid,
			st.linenumber,
			st.sublinenumber,
			et.eventtypeid,
			st.compensationdate,
			st.salestransactionseq,
			st.alternateordernumber,
			:v_periodRow.periodseq AS periodseq,
			sta.positionname,
			0 AS positioinseq,
			9999999999 AS creditvalue,
			0 AS unittypeforcreditvalue,
			st.genericdate6 AS lastpaymentdate
		FROM
			cs_salesorder so
		INNER JOIN cs_salestransaction st ON
			st.salesorderseq = so.salesorderseq
		INNER JOIN cs_transactionassignment sta	ON
			st.salestransactionseq = sta.salestransactionseq
			AND st.compensationdate = sta.compensationdate
		INNER JOIN cs_eventtype et	ON
			et.datatypeseq = st.eventtypeseq
		WHERE
			so.removedate = v_removeDate
			AND et.removedate = v_removeDate
			AND et.eventtypeid = v_policyPay_ET
			AND st.compensationdate >= :v_periodRow.startdate
			AND st.compensationdate < :v_periodRow.enddate
	);
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Inserting Policy Payment transactions for the month into temp table',v_sqlCount);
	COMMIT;

	UPDATE
		ext.TRYG_SH_SALESTXNS_POLICYCRDS pc
	SET
		positionseq = (
		SELECT
			ruleelementownerseq
		FROM
			cs_position pos
		WHERE
			pos.removedate = v_removedate
			AND pos.name = pc.positionname
			AND pos.effectivestartdate <= :v_periodRow.startdate
			AND pos.effectiveenddate > :v_periodRow.startdate)
	WHERE
		periodseq = :v_periodRow.periodseq
		AND positionseq = 0;
		
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Update of positionseq using positionname on assignments',v_sqlCount);
	COMMIT;
	
	/*
	---------------------------------------------------------------------------------------------------------------------------
	Fix for Position Change 
	
	Defect TBVS2-1230
	
	A M Robinson 23/03/2023
	---------------------------------------------------------------------------------------------------------------------------
	*/

	UPDATE
		pc
	SET
		pc.positionseq=pos2.ruleelementownerseq,
		pc.positionname=pos2.name
	from 
		ext.TRYG_SH_SALESTXNS_POLICYCRDS pc
		join cs_period per on per.periodseq=pc.periodseq and per.removedate=v_removedate
		join cs_position pos on pos.ruleelementownerseq=pc.positionseq and pos.removedate=v_removedate AND pos.EFFECTIVEENDDATE >= per.STARTDATE AND pos.EFFECTIVESTARTDATE < per.endDATE
		join cs_payee pay on pay.payeeseq=pos.payeeseq and pay.removedate=v_removedate AND pay.EFFECTIVEENDDATE >= per.STARTDATE AND pay.EFFECTIVESTARTDATE < per.endDATE
		join cs_participant part on part.payeeseq=pay.payeeseq and part.removedate=v_removedate AND part.EFFECTIVEENDDATE >= per.STARTDATE AND part.EFFECTIVESTARTDATE < per.endDATE
		join cs_position pos2 on pos2.payeeseq=pay.payeeseq and pos2.removedate=v_removedate AND pos2.EFFECTIVEENDDATE >= per.STARTDATE AND pos2.EFFECTIVESTARTDATE < per.endDATE and pos2.effectivestartdate>=v_changeDate
	where pos.name <> pos2.name
	and pc.periodseq = :v_periodRow.periodseq;
		
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Update of Positions post 01/01/2023 Position Name Change',v_sqlCount);
	COMMIT;
	
	/* Clean up Duplicates */
	
	delete
    FROM ext.TRYG_SH_SALESTXNS_POLICYCRDS x
    WHERE "$rowid$"  NOT IN
    (
        SELECT MAX("$rowid$" )
        FROM ext.TRYG_SH_SALESTXNS_POLICYCRDS
        GROUP BY salestransactionseq, positionname
    );
		
	/*	
	---------------------------------------------------------------------------------------------------------------------------
	*/

	UPDATE ext.TRYG_SH_SALESTXNS_POLICYCRDS pc
	SET
		(creditvalue,unittypeforcreditvalue) = 
		(
			SELECT
				sum(IFNULL(cc.value,0)),
				1970324836974600 --cc.unittypeforvalue
			FROM
				cs_credit cc
			INNER JOIN cs_salestransaction st ON
				st.salestransactionseq = cc.salestransactionseq
			INNER JOIN cs_eventtype et ON 
				et.datatypeseq = st.eventtypeseq
			WHERE
				st.compensationdate <  pc.compensationdate
				AND st.compensationdate >= pc.LASTPAYMENTDATE
				AND et.removedate = v_removedate
				AND et.eventtypeid IN (v_policySales_ET	,v_policySalesSummary_ET) --summary transactions have been included
				AND st.alternateordernumber = pc.alternateordernumber
				AND cc.positionseq in
					(	select distinct posall.ruleelementownerseq
						from cs_position poscr
						join cs_period per on poscr.EFFECTIVEENDDATE >= per.STARTDATE AND poscr.EFFECTIVESTARTDATE < per.endDATE and per.removedate=v_removedate
						join cs_payee pay on pay.payeeseq=poscr.payeeseq and pay.removedate=v_removedate AND pay.EFFECTIVEENDDATE >= per.STARTDATE AND pay.EFFECTIVESTARTDATE < per.endDATE
						join cs_participant part on part.payeeseq=pay.payeeseq and part.removedate=v_removedate AND part.EFFECTIVEENDDATE >= per.STARTDATE AND part.EFFECTIVESTARTDATE < per.endDATE
						join cs_position posall on posall.payeeseq=pay.payeeseq and posall.removedate=v_removedate --AND pos2.EFFECTIVEENDDATE >= per.STARTDATE AND pos2.EFFECTIVESTARTDATE < per.endDATE and pos2.effectivestartdate>=to_date('01012023','ddmmyyyy')
						where poscr.ruleelementownerseq=pc.positionseq and per.periodseq=cc.periodseq
					) -- = pc.positionseq
			GROUP BY 
				1970324836974600 --cc.unittypeforvalue
		)
	WHERE
		periodseq = :v_periodRow.periodseq
		AND creditvalue = 9999999999;
	
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Update of credit value for Policy Pay TXNS using ALTERNATEORDER number on policy sales TXNS',v_sqlCount);
	COMMIT;


	UPDATE
		cs_transactionassignment sta
	SET
		(genericnumber1,unittypeforgenericnumber1,genericboolean1) = (
			SELECT
				cc.creditvalue,
				cc.unittypeforcreditvalue,
				1
			FROM
				TRYG_SH_SALESTXNS_POLICYCRDS cc
			WHERE 
				cc.periodseq = :v_periodRow.periodseq
				AND cc.SALESTRANSACTIONSEQ = sta.SALESTRANSACTIONSEQ
				AND cc.positionname = sta.positionname
				group by cc.creditvalue, cc.unittypeforcreditvalue, 1
		)
	WHERE
		EXISTS 
		(
			SELECT
				1
			FROM
				ext.TRYG_SH_SALESTXNS_POLICYCRDS cc
			WHERE
				cc.periodseq = :v_periodRow.periodseq
				AND cc.SALESTRANSACTIONSEQ = sta.SALESTRANSACTIONSEQ
		);
	
	v_sqlCount = ::ROWCOUNT;
	CALL EXT.TRYG_LOG(v_procedureName,'Update of credit value on assignment of policy payment TXNS',v_sqlCount);
	COMMIT;


	COMMIT;
	EXT.TRYG_LOG(v_procedureName,'####   END   ####',NULL);

	
END