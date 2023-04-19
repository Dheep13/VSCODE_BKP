CREATE PROCEDURE EXT.TRYG_SH_PORTFOLIO ( in_PeriodSeq BIGINT,in_ProcessingUnitSeq BIGINT) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
DEFAULT SCHEMA EXT AS 
/*---------------------------------------------------------------------
    | Author: Sharath K
    | Project Title: Consultant
    | Company: SAP Callidus
    | Initial Version Date: 01-Jan-2023
    |----------------------------------------------------------------------
    | Procedure Purpose: 
    | Version: 0.1	01-Jan-2023	Intial Version
    -----------------------------------------------------------------------
    */
BEGIN
	--Row type variables declarations
	DECLARE v_periodRow ROW LIKE TCMP.CS_PERIOD;
	DECLARE v_puRow ROW LIKE TCMP.CS_PROCESSINGUNIT;
	DECLARE v_eventTypeRow ROW LIKE TCMP.CS_EVENTTYPE;


	--Variable declarations
	DECLARE v_tenantid VARCHAR(50);
	DECLARE v_procedureName VARCHAR(50);
	DECLARE v_slqerrm VARCHAR(4000);
	DECLARE v_eventType VARCHAR(50);

	DECLARE v_removeDate DATE;

	DECLARE v_sqlCount INT;
	DECLARE v_monthDiff INT;



	-- Exeception Handling
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN v_slqerrm := ::SQL_ERROR_MESSAGE;
		CALL EXT.TRYG_LOG(v_procedureName,'ERROR = '||IFNULL(:v_slqerrm,'') ,NULL);
	END;

	--------------------------------------------------------------------------- 
	v_procedureName = 'TRYG_SH_PORTFOLIO';
	v_eventType = 'SC-DK-002-004';
	v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');

	SELECT * INTO v_puRow FROM TCMP.CS_PROCESSINGUNIT cp WHERE cp.PROCESSINGUNITSEQ = in_ProcessingUnitSeq;
	SELECT * INTO v_periodRow FROM TCMP.CS_PERIOD cp WHERE cp.PERIODSEQ = in_PeriodSeq AND cp.REMOVEDATE = v_removeDate;
	SELECT * INTO v_eventTypeRow FROM TCMP.CS_EVENTTYPE et WHERE et.REMOVEDATE = v_removeDate AND et.eventtypeid = v_eventType ;

	v_tenantid = :v_puRow.TENANTID;


	EXT.TRYG_LOG(v_procedureName,'####   BEGIN   #### '||:v_periodRow.Name,NULL);

	SELECT value INTO v_monthDiff 
	FROM cs_fixedvalue fv
	WHERE fv.removedate = v_removeDate
	AND name = 'FV_Move and Departure Validity Period'
	AND fv.effectivestartdate <= :v_periodRow.enddate
	AND fv.effectiveenddate > :v_periodRow.enddate;

	EXT.TRYG_LOG(v_procedureName,'get Month Difference '||:v_monthDiff,NULL);


	UPDATE cs_salestransaction st
	SET genericboolean1 = 1
	WHERE st.eventtypeseq =  :v_eventTypeRow.datatypeseq
	AND st.genericattribute8 IN ('TA-','TA') -- departures
	AND st.compensationdate >= :v_periodRow.startDate
	AND st.compensationdate < :v_periodRow.enddate
	AND EXISTS (
		SELECT 1 FROM cs_salestransaction st_in
		WHERE st_in.eventtypeseq =  :v_eventTypeRow.datatypeseq
		AND st_in.genericattribute8 IN ('AG+','AG-') -- Relocations
		AND months_between(st_in.compensationdate,st.compensationdate)<=:v_monthDiff
		AND st_in.alternateordernumber = st.alternateordernumber
	);

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericboolean1 for Departures Transaction with existing relocation transaction',v_sqlCount);	
	COMMIT;


	COMMIT;
	EXT.TRYG_LOG(v_procedureName,'####   END   ####',NULL);

	
END