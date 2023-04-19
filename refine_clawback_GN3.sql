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
							AND et_lp.removedate = to_date('22000101','yyyymmdd')  
						WHERE 
							st_lp.alternateordernumber = st.alternateordernumber
                            AND sta_lp.processingunitseq= 38280596832649318
							AND sta_lp.positionname = sta.positionname 
							AND et_lp.eventtypeid = 'SC-DK-001-001-SUMMARY'
							AND st_lp.compensationdate < st.compensationdate
							AND st_lp.compensationdate = (   
										SELECT max(st_lp_in.compensationdate) 
										FROM cs_salestransaction st_lp_in 	
										INNER JOIN cs_transactionassignment sta_lp_in ON
											sta_lp_in.salestransactionseq = st_lp_in.salestransactionseq
											AND sta_lp_in.compensationdate = st_lp_in.compensationdate
										INNER JOIN cs_eventtype et_lp_in ON
											et_lp_in.datatypeseq = st_lp_in.eventtypeseq
											AND et_lp_in.removedate = to_date('22000101','yyyymmdd')  
										WHERE 
											st_lp_in.alternateordernumber = st.alternateordernumber
                                            AND sta_lp_in.processingunitseq= 38280596832649318
											AND sta_lp_in.positionname = sta_lp.positionname
											AND et_lp_in.eventtypeid = 'SC-DK-001-001-SUMMARY'
											AND st_lp_in.compensationdate < st.compensationdate
                                            AND st.sublinenumber = st_lp_in.sublinenumber
											)) lastest_premium
				FROM
					cs_salestransaction st
				INNER JOIN cs_transactionassignment sta ON
					sta.salestransactionseq = st.salestransactionseq
					AND sta.compensationdate = st.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st.eventtypeseq
					AND et.removedate = to_date('22000101','yyyymmdd')
				WHERE
					st.genericdate3 IS NULL
                    AND sta.processingunitseq= 38280596832649318
					AND st.genericnumber1 > st.genericnumber2  -- Old Premium Less than NEW Premium FOR NEW AND Increase txns
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st.compensationdate >= to_date('20220601','yyyymmdd')
					AND st.compensationdate < to_date('20220630','yyyymmdd')
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
					AND et.removedate = to_date('22000101','yyyymmdd')
				LEFT JOIN cs_credit cc ON
					cc.salestransactionseq = st.salestransactionseq
				LEFT JOIN cs_position pos ON	
					pos.name = sta.positionname
					AND pos.removedate = to_date('22000101','yyyymmdd')
					AND pos.effectiveenddate > current_date
					AND pos.effectivestartdate <= current_date
					AND pos.ruleelementownerseq = cc.positionseq
				WHERE
					st.genericdate3 IS NULL
                    AND sta.processingunitseq= 38280596832649318
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st.compensationdate >= add_months(to_date('20220601','yyyymmdd'),-12)
					AND st.compensationdate < to_date('20220601','yyyymmdd')
					AND IFNULL(cc.periodseq, 2533274790396037) <= 2533274790396037
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
			tgt.unittypeforgenericnumber2 =  1970324836974600,
			tgt.genericnumber3 = src.lastest_premium,
			tgt.unittypeforgenericnumber3 = 1970324836974600
		;








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
							AND et_lp.removedate = to_date('01/01/2200','mm/dd/yyyy')  
						WHERE 
							st_lp.alternateordernumber = st.alternateordernumber
                            AND sta_lp.processingunitseq= 38280596832649318
							AND sta_lp.positionname = sta.positionname 
							AND et_lp.eventtypeid = 'SC-DK-001-001-SUMMARY'
							AND st_lp.compensationdate < st.compensationdate
							AND st_lp.compensationdate = (   
										SELECT max(st_lp_in.compensationdate) 
										FROM cs_salestransaction st_lp_in 	
										INNER JOIN cs_transactionassignment sta_lp_in ON
											sta_lp_in.salestransactionseq = st_lp_in.salestransactionseq
											AND sta_lp_in.compensationdate = st_lp_in.compensationdate
										INNER JOIN cs_eventtype et_lp_in ON
											et_lp_in.datatypeseq = st_lp_in.eventtypeseq
											AND et_lp_in.removedate = to_date('01/01/2200','mm/dd/yyyy')  
										WHERE 
											st_lp_in.alternateordernumber = st.alternateordernumber
                                            AND sta_lp_in.processingunitseq= 38280596832649318
											AND sta_lp_in.positionname = sta_lp.positionname
											AND et_lp_in.eventtypeid = 'SC-DK-001-001-SUMMARY'
											AND st_lp_in.compensationdate < st.compensationdate
											) GROUP BY st_lp.alternateordernumber, st_lp.compensationdate) MAX_SUB
                                            INNER JOIN CS_SALESTRANSACTION st_sub on
										 st_sub.compensationdate=MAX_SUB.compensationdate 
										 AND MAX_SUB.alternateordernumber = st_sub.alternateordernumber
										 AND MAX_SUB.sublinenumber=st_sub.sublinenumber
										 INNER JOIN cs_eventtype et_sub ON
											et_sub.datatypeseq = st_sub.eventtypeseq
											AND et_sub.removedate = to_date('01/01/2200','mm/dd/yyyy') 
											where et_sub.eventtypeid = 'SC-DK-001-001-SUMMARY') lastest_premium
				FROM
					cs_salestransaction st
				INNER JOIN cs_transactionassignment sta ON
					sta.salestransactionseq = st.salestransactionseq
					AND sta.compensationdate = st.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st.eventtypeseq
					AND et.removedate = to_date('01/01/2200','mm/dd/yyyy')
				WHERE
					st.genericdate3 IS NULL
                    AND sta.processingunitseq= 38280596832649318
					AND st.genericnumber1 > st.genericnumber2  -- Old Premium Less than NEW Premium FOR NEW AND Increase txns
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st.compensationdate >= to_date('06/01/2022','mm/dd/yyyy')
					AND st.compensationdate < to_date('06/23/2022','mm/dd/yyyy')
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
					st.genericdate3 IS NULL
                    AND sta.processingunitseq= 38280596832649318
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st.compensationdate >= add_months(to_date('06/01/2022','mm/dd/yyyy'),-12)
					AND st.compensationdate < to_date('06/01/2022','mm/dd/yyyy')
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
			tgt.unittypeforgenericnumber2 =  1970324836974600,
			tgt.genericnumber3 = src.lastest_premium,
			tgt.unittypeforgenericnumber3 = 1970324836974600
		;