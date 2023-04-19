CREATE PROCEDURE EXT.TRYG_IB_SALESTXNS ( in_batchName varchar(100) ) 
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
	DECLARE v_et_policySalesRow ROW LIKE TCMP.CS_EVENTTYPE ;



	--Variable declarations
	DECLARE v_tenantid VARCHAR(50);
	DECLARE v_procedureName VARCHAR(50);
	DECLARE v_slqerrm VARCHAR(4000);
	DECLARE v_et_policySales VARCHAR(50);
	DECLARE v_et_policyPay VARCHAR(50);

	DECLARE v_sqlCount INT;
	DECLARE v_maxSalesTxnSeq BIGINT;

	DECLARE v_removeDate DATE;


	-- Exeception Handling
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN v_slqerrm := ::SQL_ERROR_MESSAGE;
		CALL EXT.TRYG_LOG(v_procedureName,'ERROR = '||IFNULL(:v_slqerrm,'') ,NULL);
	END;

	--------------------------------------------------------------------------- 
	v_procedureName = 'TRYG_IB_SALESTXNS';
	v_et_policySales = 'SC-DK-001-001';
	v_et_policyPay = 'SC-DK-001-002';
	v_removeDate = TO_DATE('01/01/2200','mm/dd/yyyy');

	SELECT * INTO v_et_policySalesRow FROM TCMP.CS_EVENTTYPE et WHERE et.REMOVEDATE = v_removeDate AND et.eventtypeid = v_et_policySales;

--	v_tenantid = :v_puRow.TENANTID;

	EXT.TRYG_LOG(v_procedureName,'####   BEGIN   #### ',NULL);
	EXT.TRYG_LOG(v_procedureName,'batchName = '||in_batchName,NULL);


	DELETE FROM TRYG_IB_SALESTXNS_POLICYASSIGNS 
	WHERE batchname = in_batchName;
	v_sqlCount = ::ROWCOUNT;  
	CALL EXT.TRYG_LOG(v_procedureName,'Delete assignments in temp table for same batch complete',v_sqlCount);
	COMMIT;	

	INSERT INTO ext.TRYG_IB_SALESTXNS_POLICYASSIGNS
	(
		SELECT
			:in_batchname,
			stp.ORDERID ,
			stp.LINENUMBER ,
			stp.SUBLINENUMBER ,
			stp.EVENTTYPEID ,
			sta.positionname,
			st.alternateordernumber
		FROM
			cs_salestransaction st
		INNER JOIN cs_transactionassignment sta
		ON
			st.salestransactionseq = sta.salestransactionseq
			AND st.compensationdate = sta.compensationdate 
		INNER JOIN ext.TRYG_IB_SALESTXNS_POLICYPAY stp
		ON
			stp.ALTERNATEORDERNUMBER = st.ALTERNATEORDERNUMBER
			AND stp.COMPENSATIONDATE > st.COMPENSATIONDATE
		WHERE
			st.eventtypeseq  = :v_et_policySalesRow.datatypeseq
		GROUP BY
			stp.ORDERID ,
			stp.LINENUMBER ,
			stp.SUBLINENUMBER ,
			stp.EVENTTYPEID ,
			st.alternateordernumber,
			sta.positionname
	);
	v_sqlCount = ::ROWCOUNT;  
	CALL EXT.TRYG_LOG(v_procedureName,'insert transactions assignment in temp table for matching ALTERNATEORDERNUMBER',v_sqlCount);
	COMMIT;	

	SELECT max(stagesalestransactionseq)
	INTO v_maxSalesTxnSeq
	FROM cs_stagesalestransaction;
	CALL EXT.TRYG_LOG(v_procedureName,'Get the max stage salestransaction seq ',v_maxSalesTxnSeq);

	UPDATE
		TRYG_IB_SALESTXNS_POLICYPAY pp
	SET
		genericdate6 = to_date('01/01/2000','mm/dd/yyyy');
	
	v_sqlCount = ::ROWCOUNT;  
	CALL EXT.TRYG_LOG(v_procedureName,'Upadate GD6(lastpayment date) to default start date of 01/01/2000',v_sqlCount);
	COMMIT;	

	UPDATE
		TRYG_IB_SALESTXNS_POLICYPAY pp
	SET
		genericdate6 = (
			SELECT
				IFNULL( max(compensationdate),to_date('01/01/2000','mm/dd/yyyy'))
			FROM
				cs_salestransaction st
			INNER JOIN cs_salesorder so ON 
				so.salesorderseq = st.salesorderseq
			INNER JOIN cs_eventtype et ON
				et.datatypeseq = st.eventtypeseq
			WHERE 
					so.removedate = v_removedate
				AND et.removedate = v_removedate
				AND st.alternateordernumber = pp.alternateordernumber
				AND et.eventtypeid = pp.eventtypeid
				AND st.compensationdate < pp.compensationdate
		)
	WHERE 
		EXISTS (
			SELECT
				1
			FROM
				cs_salestransaction st
			INNER JOIN cs_salesorder so ON 
				so.salesorderseq = st.salesorderseq
			INNER JOIN cs_eventtype et ON
				et.datatypeseq = st.eventtypeseq
			WHERE 
					so.removedate = v_removedate
				AND et.removedate = v_removedate
				AND st.alternateordernumber = pp.alternateordernumber
				AND et.eventtypeid = pp.eventtypeid
				AND st.compensationdate <= pp.compensationdate
			);
	v_sqlCount = ::ROWCOUNT;  
	CALL EXT.TRYG_LOG(v_procedureName,'Upadate GD6(lastpayment date) to max of compensationdate if same alternate order number found',v_sqlCount);
	COMMIT;	

	DELETE FROM cs_stagesalestransaction 
	WHERE batchname = in_batchName;
	v_sqlCount = ::ROWCOUNT;  
	CALL EXT.TRYG_LOG(v_procedureName,'Delete stage transactions for same batch complete',v_sqlCount);
	COMMIT;	

	DELETE FROM cs_stagesalestransaction 
	WHERE (orderid,linenumber,sublinenumber,eventtypeid) IN 
		  (SELECT orderid,linenumber,sublinenumber,eventtypeid FROM TRYG_IB_SALESTXNS_POLICYPAY );
	v_sqlCount = ::ROWCOUNT;  
	CALL EXT.TRYG_LOG(v_procedureName,'Delete stage transactions for same orderid,linenumber,sublinenumber,eventtypeid complete',v_sqlCount);
	COMMIT;	


	INSERT INTO	cs_stagesalestransaction
	(
		STAGESALESTRANSACTIONSEQ,
		BATCHNAME,
		ORDERID,
		LINENUMBER,
		SUBLINENUMBER,
		EVENTTYPEID,
		ACCOUNTINGDATE,
		PRODUCTID,
		PRODUCTNAME,
		PRODUCTDESCRIPTION,
		VALUE,
		UNITTYPEFORVALUE,
		NUMBEROFUNITS,
		UNITVALUE,
		UNITTYPEFORUNITVALUE,
		COMPENSATIONDATE,
		PAYMENTTERMS,
		PONUMBER,
		CHANNEL,
		ALTERNATEORDERNUMBER,
		DATASOURCE,
		NATIVECURRENCY,
		NATIVECURRENCYAMOUNT,
		DISCOUNTPERCENT,
		DISCOUNTTYPE,
		REASONID,
		COMMENTS,
		STAGEPROCESSDATE,
		STAGEPROCESSFLAG,
		BUSINESSUNITNAME,
		GENERICATTRIBUTE1,
		GENERICATTRIBUTE2,
		GENERICATTRIBUTE3,
		GENERICATTRIBUTE4,
		GENERICATTRIBUTE5,
		GENERICATTRIBUTE6,
		GENERICATTRIBUTE7,
		GENERICATTRIBUTE8,
		GENERICATTRIBUTE9,
		GENERICATTRIBUTE10,
		GENERICATTRIBUTE11,
		GENERICATTRIBUTE12,
		GENERICATTRIBUTE13,
		GENERICATTRIBUTE14,
		GENERICATTRIBUTE15,
		GENERICATTRIBUTE16,
		GENERICATTRIBUTE17,
		GENERICATTRIBUTE18,
		GENERICATTRIBUTE19,
		GENERICATTRIBUTE20,
		GENERICATTRIBUTE21,
		GENERICATTRIBUTE22,
		GENERICATTRIBUTE23,
		GENERICATTRIBUTE24,
		GENERICATTRIBUTE25,
		GENERICATTRIBUTE26,
		GENERICATTRIBUTE27,
		GENERICATTRIBUTE28,
		GENERICATTRIBUTE29,
		GENERICATTRIBUTE30,
		GENERICATTRIBUTE31,
		GENERICATTRIBUTE32,
		GENERICNUMBER1,
		UNITTYPEFORGENERICNUMBER1,
		GENERICNUMBER2,
		UNITTYPEFORGENERICNUMBER2,
		GENERICNUMBER3,
		UNITTYPEFORGENERICNUMBER3,
		GENERICNUMBER4,
		UNITTYPEFORGENERICNUMBER4,
		GENERICNUMBER5,
		UNITTYPEFORGENERICNUMBER5,
		GENERICNUMBER6,
		UNITTYPEFORGENERICNUMBER6,
		GENERICDATE1,
		GENERICDATE2,
		GENERICDATE3,
		GENERICDATE4,
		GENERICDATE5,
		GENERICDATE6,
		GENERICBOOLEAN1,
		GENERICBOOLEAN2,
		GENERICBOOLEAN3,
		GENERICBOOLEAN4,
		GENERICBOOLEAN5,
		GENERICBOOLEAN6)
	(
	SELECT
		v_maxSalesTxnSeq + ROW_NUMBER() OVER (ORDER BY ORDERID,LINENUMBER,SUBLINENUMBER,EVENTTYPEID) ,
		:in_batchname,
		ORDERID,
		LINENUMBER,
		SUBLINENUMBER,
		EVENTTYPEID,
		ACCOUNTINGDATE,
		PRODUCTID,
		PRODUCTNAME,
		PRODUCTDESCRIPTION,
		IFNULL(VALUE,0),
		IFNULL(UNITTYPEFORVALUE,'DKK'),
		NUMBEROFUNITS,
		UNITVALUE,
		UNITTYPEFORUNITVALUE,
		COMPENSATIONDATE,
		PAYMENTTERMS,
		PONUMBER,
		CHANNEL,
		ALTERNATEORDERNUMBER,
		DATASOURCE,
		NATIVECURRENCY,
		NATIVECURRENCYAMOUNT,
		DISCOUNTPERCENT,
		DISCOUNTTYPE,
		REASONID,
		COMMENTS,
		NULL,
		0,
		BUSINESSUNITNAME,
		GENERICATTRIBUTE1,
		GENERICATTRIBUTE2,
		GENERICATTRIBUTE3,
		GENERICATTRIBUTE4,
		GENERICATTRIBUTE5,
		GENERICATTRIBUTE6,
		GENERICATTRIBUTE7,
		GENERICATTRIBUTE8,
		GENERICATTRIBUTE9,
		GENERICATTRIBUTE10,
		GENERICATTRIBUTE11,
		GENERICATTRIBUTE12,
		GENERICATTRIBUTE13,
		GENERICATTRIBUTE14,
		GENERICATTRIBUTE15,
		GENERICATTRIBUTE16,
		GENERICATTRIBUTE17,
		GENERICATTRIBUTE18,
		GENERICATTRIBUTE19,
		GENERICATTRIBUTE20,
		GENERICATTRIBUTE21,
		GENERICATTRIBUTE22,
		GENERICATTRIBUTE23,
		GENERICATTRIBUTE24,
		GENERICATTRIBUTE25,
		GENERICATTRIBUTE26,
		GENERICATTRIBUTE27,
		GENERICATTRIBUTE28,
		GENERICATTRIBUTE29,
		GENERICATTRIBUTE30,
		GENERICATTRIBUTE31,
		GENERICATTRIBUTE32,
		GENERICNUMBER1,
		UNITTYPEFORGENERICNUMBER1,
		GENERICNUMBER2,
		UNITTYPEFORGENERICNUMBER2,
		GENERICNUMBER3,
		UNITTYPEFORGENERICNUMBER3,
		GENERICNUMBER4,
		UNITTYPEFORGENERICNUMBER4,
		GENERICNUMBER5,
		UNITTYPEFORGENERICNUMBER5,
		GENERICNUMBER6,
		UNITTYPEFORGENERICNUMBER6,
		GENERICDATE1,
		GENERICDATE2,
		GENERICDATE3,
		GENERICDATE4,
		GENERICDATE5,
		GENERICDATE6,
		GENERICBOOLEAN1,
		GENERICBOOLEAN2,
		GENERICBOOLEAN3,
		GENERICBOOLEAN4,
		GENERICBOOLEAN5,
		GENERICBOOLEAN6
	FROM
		ext.TRYG_IB_SALESTXNS_POLICYPAY 
	);

	v_sqlCount = ::ROWCOUNT;  
	CALL EXT.TRYG_LOG(v_procedureName,'Insert Policy Pay transactions into stage table',v_sqlCount);
	COMMIT;	

	DELETE FROM cs_stagetransactionassign 
	WHERE batchname = in_batchName;
	v_sqlCount = ::ROWCOUNT;  
	CALL EXT.TRYG_LOG(v_procedureName,'Delete stage transactions assignments for same batch complete',v_sqlCount);
	COMMIT;	

	DELETE FROM cs_stagetransactionassign 
	WHERE (orderid,linenumber,sublinenumber,eventtypeid) IN 
		  (SELECT orderid,linenumber,sublinenumber,eventtypeid FROM TRYG_IB_SALESTXNS_POLICYPAY );
	v_sqlCount = ::ROWCOUNT;  
	CALL EXT.TRYG_LOG(v_procedureName,'Delete stage transactions assignments for same orderid,linenumber,sublinenumber,eventtypeid complete',v_sqlCount);
	COMMIT;	

	INSERT INTO	cs_stagetransactionassign sta
	(
	 	STAGESALESTRANSACTIONSEQ,
		SETNUMBER,
		BATCHNAME,
		ORDERID,
		LINENUMBER,
		SUBLINENUMBER,
		EVENTTYPEID,
		POSITIONNAME
	)
	(
		SELECT
			st.STAGESALESTRANSACTIONSEQ,
			ROW_NUMBER () OVER (ORDER BY st.STAGESALESTRANSACTIONSEQ),
			st.batchname,
			stp.orderid,
			stp.linenumber,
			stp.sublinenumber,
			stp.eventtypeid,
			stp.positionname
		FROM
			cs_stagesalestransaction st
		INNER JOIN
			ext.TRYG_IB_SALESTXNS_POLICYASSIGNS stp
			ON
				stp.orderid = st.orderid
			AND stp.linenumber = st.linenumber
		    AND stp.sublinenumber = st.sublinenumber
			AND stp.eventtypeid = st.eventtypeid
		WHERE
			st.batchname = in_batchname
			AND st.batchname = stp.batchname
	);
	v_sqlCount = ::ROWCOUNT;  
	CALL EXT.TRYG_LOG(v_procedureName,'Insert Policy Pay transactions assignments into stage table',v_sqlCount);
	COMMIT;	



	COMMIT;
	EXT.TRYG_LOG(v_procedureName,'####   END   ####',NULL);

	
END