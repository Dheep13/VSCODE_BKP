select * from
		cs_transactionassignment tgt
		
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
                    SELECT DISTINCT st_sub.genericnumber2
                    FROM (
                            SELECT ROW_NUMBER() OVER (
                                    PARTITION BY st_l1.alternateordernumber
                                    ORDER BY st_l1.compensationdate DESC
                                ) row_num,
                                st_l1.sublinenumber,
                                st_l1.alternateordernumber,
                                st_l1.compensationdate
                            FROM cs_salestransaction st_l1,
                                (
                                
                                	
                                    SELECT DISTINCT st_in.alternateordernumber,
                                        st_in.compensationdate,
                                        sta_in.positionname
                                    FROM cs_salestransaction st_in
                                        INNER JOIN cs_transactionassignment sta_in ON sta_in.salestransactionseq = st_in.salestransactionseq
															 
                                        AND sta_in.compensationdate = st_in.compensationdate
                                        INNER JOIN cs_eventtype et ON et.datatypeseq = st_in.eventtypeseq
											 
                                        AND et.removedate =  to_date('01/01/2200','mm/dd/yyyy')
                                    WHERE st_in.genericdate3 IS NOT  NULL
														   
                                        AND sta_in.processingunitseq = 38280596832649318
                                        AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid ='SC-DK-001-001-SUMMARY'
                                        AND st_in.compensationdate >= to_date('03/01/2022','mm/dd/yyyy')
                                        AND st_in.compensationdate <to_date('03/31/2023','mm/dd/yyyy')
                                ) decr_txn,
                                cs_transactionassignment ta_l1,
                                cs_eventtype et_l1
                            where st_l1.compensationdate < decr_txn.compensationdate
                                and st_l1.alternateordernumber = decr_txn.alternateordernumber
                                and ta_l1.positionname = decr_txn.positionname
                                and ta_l1.salestransactionseq = st_l1.salestransactionseq
                                and st_l1.eventtypeseq = et_l1.datatypeseq
                                and et_l1.eventtypeid = 'SC-DK-001-001-SUMMARY'
                                and et_l1.removedate =  to_date('01/01/2200','mm/dd/yyyy')
                                and st_l1.genericdate3 is null
                                and st_l1.alternateordernumber =st.alternateordernumber 
                            group by st_l1.alternateordernumber,
                                st_l1.sublinenumber,
                                st_l1.compensationdate
                        ) max_sub
                        inner join cs_salestransaction st_sub on st_sub.compensationdate = MAX_SUB.compensationdate
                        AND MAX_SUB.alternateordernumber = st_sub.alternateordernumber
                        AND MAX_SUB.sublinenumber = st_sub.sublinenumber
                        INNER JOIN cs_eventtype et_sub ON et_sub.datatypeseq = st_sub.eventtypeseq
                        AND et_sub.removedate = to_date('01/01/2200','mm/dd/yyyy')
                    WHERE et_sub.eventtypeid = 'SC-DK-001-001-SUMMARY'
                        AND MAX_SUB.row_num = 1
                )  lastest_premium
				FROM
					cs_salestransaction st
				INNER JOIN cs_transactionassignment sta ON
					sta.salestransactionseq = st.salestransactionseq
					AND sta.compensationdate = st.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st.eventtypeseq
					AND et.removedate =  to_date('01/01/2200','mm/dd/yyyy')
				WHERE
					st.genericdate3 IS NULL
                    AND sta.processingunitseq= 38280596832649318
					AND st.genericattribute1 = 'AFGA'
					AND st.genericnumber1 > st.genericnumber2  -- Old Premium Less than NEW Premium FOR NEW AND Increase txns
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st.compensationdate >=to_date('03/01/2022','mm/dd/yyyy')
					AND st.compensationdate < to_date('03/31/2022','mm/dd/yyyy')
--					AND st.genericattribute10 IS null
					-- AND alternateordernumber = '6700008050377'
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
					AND et.removedate = to_date('01/01/2200','mm/dd/yyyy')
				LEFT JOIN cs_credit cc ON
					cc.salestransactionseq = st.salestransactionseq
				LEFT JOIN cs_position pos ON	
					pos.name = sta.positionname
					AND pos.removedate = to_date('01/01/2200','mm/dd/yyyy')
					AND pos.effectiveenddate > current_date
					AND pos.effectivestartdate <= current_date
					AND pos.ruleelementownerseq = cc.positionseq
				WHERE
					st.genericdate3 IS NOT NULL
                    AND sta.processingunitseq= 38280596832649318
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st.compensationdate >= add_months(to_date('03/01/2022','mm/dd/yyyy'),-12)
					AND st.compensationdate < to_date('03/31/2022','mm/dd/yyyy')
					AND IFNULL(cc.periodseq, 2533274790396034) <= 2533274790396034
					-- AND alternateordernumber = '6700008050377'	
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
											   						 
											   
		 
		), src  
		where 
			src.cantxns_salestransactionseq = tgt.SALESTRANSACTIONSEQ
			AND src.cantxns_positionname = tgt.positionname
		;

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericnumber2 for Cancelled txns with credit value',v_sqlCount);	
	COMMIT;


select * from cs_period where name like 'March 2022';





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
                    SELECT DISTINCT st_sub.genericnumber2
                    FROM (
                            SELECT ROW_NUMBER() OVER (
                                    PARTITION BY st_l1.alternateordernumber
                                    ORDER BY st_l1.compensationdate DESC
                                ) row_num,
                                st_l1.sublinenumber,
                                st_l1.alternateordernumber,
                                st_l1.compensationdate
                            FROM cs_salestransaction st_l1,
                                (
                                    SELECT DISTINCT st_in.alternateordernumber,
                                        st_in.compensationdate,
                                        sta_in.positionname
                                    FROM cs_salestransaction st_in
                                        INNER JOIN cs_transactionassignment sta_in ON sta_in.salestransactionseq = st_in.salestransactionseq
															 
                                        AND sta_in.compensationdate = st_in.compensationdate
                                        INNER JOIN cs_eventtype et ON et.datatypeseq = st_in.eventtypeseq
											 
                                        AND et.removedate = to_date('01/01/2200','mm/dd/yyyy')
                                        WHERE st_in.genericdate3 IS NOT NULL
														   
                                        AND sta_in.processingunitseq = 
                                        AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid = :'SC-DK-001-001-SUMMARY'
                                        AND st_in.compensationdate >= to_date('12/01/2021','mm/dd/yyyy')
                                        AND st_in.compensationdate < to_date('12/31/2021','mm/dd/yyyy')
                                ) decr_txn,
                                cs_transactionassignment ta_l1,
                                cs_eventtype et_l1
                            where st_l1.compensationdate < decr_txn.compensationdate
                                and st_l1.alternateordernumber = decr_txn.alternateordernumber
                                and ta_l1.positionname = decr_txn.positionname
                                and ta_l1.salestransactionseq = st_l1.salestransactionseq
                                and st_l1.eventtypeseq = et_l1.datatypeseq
                                and et_l1.eventtypeid = :'SC-DK-001-001-SUMMARY'
                                and et_l1.removedate = to_date('01/01/2200','mm/dd/yyyy')
                                and st_l1.genericdate3 is null
                                and st_l1.alternateordernumber = st.alternateordernumber
                            group by st_l1.alternateordernumber,
                                st_l1.sublinenumber,
                                st_l1.compensationdate
                        ) max_sub
                        inner join cs_salestransaction st_sub on st_sub.compensationdate = MAX_SUB.compensationdate
                        AND MAX_SUB.alternateordernumber = st_sub.alternateordernumber
                        AND MAX_SUB.sublinenumber = st_sub.sublinenumber
                        INNER JOIN cs_eventtype et_sub ON et_sub.datatypeseq = st_sub.eventtypeseq
                        AND et_sub.removedate = to_date('01/01/2200','mm/dd/yyyy')
                    WHERE et_sub.eventtypeid = :'SC-DK-001-001-SUMMARY'
                        AND MAX_SUB.row_num = 1
                )  lastest_premium
				FROM
					cs_salestransaction st
				INNER JOIN cs_transactionassignment sta ON
					sta.salestransactionseq = st.salestransactionseq
					AND sta.compensationdate = st.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st.eventtypeseq
					AND et.removedate = to_date('01/01/2200','mm/dd/yyyy')
				WHERE
					st.genericdate3 IS NOT NULL
                    AND sta.processingunitseq= 38280596832649318
					AND st.genericattribute1 = 'AFGA'
					AND st.genericnumber1 > st.genericnumber2  -- Old Premium Less than NEW Premium FOR NEW AND Increase txns
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st.compensationdate >= to_date('12/01/2021','mm/dd/yyyy')
					AND st.compensationdate < to_date('12/31/2021','mm/dd/yyyy')
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
					AND et.removedate = to_date('01/01/2200','mm/dd/yyyy')
				LEFT JOIN cs_credit cc ON
					cc.salestransactionseq = st.salestransactionseq
				LEFT JOIN cs_position pos ON	
					pos.name = sta.positionname
					AND pos.removedate = to_date('01/01/2200','mm/dd/yyyy')
					AND pos.effectiveenddate > current_date
					AND pos.effectivestartdate <= current_date
					AND pos.ruleelementownerseq = cc.positionseq
				WHERE
					st.genericdate3 IS NOT NULL
                    AND sta.processingunitseq= 38280596832649318
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st.compensationdate >= add_months(to_date('12/01/2021','mm/dd/yyyy'),-12)
					AND st.compensationdate < to_date('12/01/2021','mm/dd/yyyy')
					AND IFNULL(cc.periodseq, 2533274790396026) <= 2533274790396026
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
			tgt.unittypeforgenericnumber2 = 1970324836974600
			tgt.genericnumber3 = src.lastest_premium,
			tgt.unittypeforgenericnumber3 = 1970324836974600
		;

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericnumber2 for Cancelled txns with credit value',v_sqlCount);	
	COMMIT;



MERGE INTO cs_transactionassignment tgt USING (
    SELECT *
    FROM (
            SELECT st.salestransactionseq AS cantxns_salestransactionseq,
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
                    SELECT DISTINCT st_sub.genericnumber2
                    FROM (
                            SELECT ROW_NUMBER() OVER (
                                    PARTITION BY st_l1.alternateordernumber
                                    ORDER BY st_l1.compensationdate DESC
                                ) row_num,
                                st_l1.sublinenumber,
                                st_l1.alternateordernumber,
                                st_l1.compensationdate
                            FROM cs_salestransaction st_l1,
                                (
                                    SELECT DISTINCT st_in.alternateordernumber,
                                        st_in.compensationdate,
                                        sta_in.positionname
                                    FROM cs_salestransaction st_in
                                        INNER JOIN cs_transactionassignment sta_in ON sta_in.salestransactionseq = st_in.salestransactionseq
                                        AND sta_in.compensationdate = st_in.compensationdate
                                        INNER JOIN cs_eventtype et ON et.datatypeseq = st_in.eventtypeseq
                                        AND et.removedate = :v_removeDate
                                    WHERE st_in.genericdate3 IS NULL
                                        AND sta_in.processingunitseq = in_ProcessingUnitSeq
                                        AND st_in.genericnumber1 > st_in.genericnumber2
                                        AND et.eventtypeid = :v_eventType
                                        AND st_in.compensationdate >= :v_periodRow.startDate
                                        AND st_in.compensationdate < :v_periodRow.endDate
                                ) decr_txn,
                                cs_transactionassignment ta_l1,
                                cs_eventtype et_l1
                            where st_l1.compensationdate < decr_txn.compensationdate
                                and st_l1.alternateordernumber = decr_txn.alternateordernumber
                                and ta_l1.positionname = decr_txn.positionname
                                and ta_l1.salestransactionseq = st_l1.salestransactionseq
                                and st_l1.eventtypeseq = et_l1.datatypeseq
                                and et_l1.eventtypeid = :v_eventType
                                and et_l1.removedate = :v_removeDate
                                and st_l1.genericdate3 is null
                                and st_l1.alternateordernumber = st.alternateordernumber
                            group by st_l1.alternateordernumber,
                                st_l1.sublinenumber,
                                st_l1.compensationdate
                        ) max_sub
                        inner join cs_salestransaction st_sub on st_sub.compensationdate = MAX_SUB.compensationdate
                        AND MAX_SUB.alternateordernumber = st_sub.alternateordernumber
                        AND MAX_SUB.sublinenumber = st_sub.sublinenumber
                        INNER JOIN cs_eventtype et_sub ON et_sub.datatypeseq = st_sub.eventtypeseq
                        AND et_sub.removedate = :v_removeDate
                    WHERE et_sub.eventtypeid = :v_eventType
                        AND MAX_SUB.row_num = 1
                ) lastest_premium
            FROM cs_salestransaction st
                INNER JOIN cs_transactionassignment sta ON sta.salestransactionseq = st.salestransactionseq
                AND sta.compensationdate = st.compensationdate
                INNER JOIN cs_eventtype et ON et.datatypeseq = st.eventtypeseq
                AND et.removedate = :v_removeDate
            WHERE st.genericdate3 IS NULL
                AND sta.processingunitseq = in_ProcessingUnitSeq
                AND st.genericnumber1 > st.genericnumber2 -- Old Premium Less than NEW Premium FOR NEW AND Increase txns
                AND et.eventtypeid = :v_eventType
                AND st.compensationdate >= :v_periodRow.startDate
                AND st.compensationdate < :v_periodRow.endDate --					AND st.genericattribute10 IS null
                -- AND alternateordernumber in (6530000872412)
        ) cantxns
        LEFT JOIN (
            SELECT st.alternateordernumber AS newtxns_alternateordernumber,
                sta.positionname AS newtxns_positionname,
                st.genericdate1 AS newtxns_policy_sDate,
                st.genericdate2 AS newtxns_policy_eDate,
                sum(IFNULL(cc.value, 0)) AS newtxns_crdvalue
            FROM cs_salestransaction st
                INNER JOIN cs_transactionassignment sta ON sta.salestransactionseq = st.salestransactionseq
                AND sta.compensationdate = st.compensationdate
                INNER JOIN cs_eventtype et ON et.datatypeseq = st.eventtypeseq
                AND et.removedate = :v_removeDate
                LEFT JOIN cs_credit cc ON cc.salestransactionseq = st.salestransactionseq
                LEFT JOIN cs_position pos ON pos.name = sta.positionname
                AND pos.removedate = :v_removeDate
                AND pos.effectiveenddate > current_date
                AND pos.effectivestartdate <= current_date
                AND pos.ruleelementownerseq = cc.positionseq
            WHERE st.genericdate3 IS NULL
                AND sta.processingunitseq = in_ProcessingUnitSeq
                AND et.eventtypeid = :v_eventType
                AND st.compensationdate >= add_months(:v_periodRow.startDate, -12)
                AND st.compensationdate < :v_periodRow.startDate
                AND IFNULL(cc.periodseq, :v_periodRow.periodseq) <= :v_periodRow.periodseq --					AND alternateordernumber = '6055002703399'	
            GROUP BY st.alternateordernumber,
                sta.positionname,
                st.genericdate1,
                st.genericdate2
        ) newtxns ON cantxns_alternateordernumber = newtxns_alternateordernumber
        AND cantxns_positionname = newtxns_positionname
        AND IFNULL(
            newtxns_policy_sDate,
            to_date('01/01/2000', 'mm/dd/yyyy')
        ) = IFNULL(
            cantxns.cantxns_policy_sDate,
            to_date('01/01/2000', 'mm/dd/yyyy')
        )
        AND IFNULL(
            newtxns_policy_eDate,
            to_date('01/01/2000', 'mm/dd/yyyy')
        ) = IFNULL(
            cantxns.cantxns_policy_eDate,
            to_date('01/01/2000', 'mm/dd/yyyy')
        )
) src ON (
    src.cantxns_salestransactionseq = tgt.SALESTRANSACTIONSEQ
    AND src.cantxns_positionname = tgt.positionname
)
WHEN MATCHED THEN
UPDATE
SET tgt.genericnumber2 = src.newtxns_crdvalue,
    tgt.unittypeforgenericnumber2 = :v_unitTypeRow.unittypeseq,
    tgt.genericnumber3 = src.lastest_premium,
    tgt.unittypeforgenericnumber3 = :v_unitTypeRow.unittypeseq;

	v_sqlCount = ::ROWCOUNT;	
	CALL EXT.TRYG_LOG(v_procedureName,'Updating the genericnumber2 for Decrease txns with credit value',v_sqlCount);	
	COMMIT;