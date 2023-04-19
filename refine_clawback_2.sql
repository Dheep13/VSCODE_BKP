




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
					(select A.genericnumber2 from (select max(st_l1.sublinenumber), st_l1.alternateordernumber, st_l1.genericnumber2, st_l1.compensationdate from cs_salestransaction st_l1 ,(
select distinct st.alternateordernumber, st.compensationdate, sta.positionname FROM
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
					AND st.genericnumber1 > st.genericnumber2 
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st.compensationdate >= to_date('06/01/2022','mm/dd/yyyy')
					AND st.compensationdate < to_date('06/30/2022','mm/dd/yyyy')) decr_txn, 
					cs_transactionassignment ta, cs_eventtype et
					where st_l1.compensationdate < decr_txn.compensationdate
					and st_l1.alternateordernumber = decr_txn.alternateordernumber
					and ta.positionname=decr_txn.positionname
					and ta.salestransactionseq=st_l1.salestransactionseq
					and st_l1.eventtypeseq=et.datatypeseq
					and  et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					and et.removedate = to_date('01/01/2200','mm/dd/yyyy')
					and st_l1.genericdate3 is  null
					and st_l1.alternateordernumber=st.alternateordernumber
					group by  st_l1.alternateordernumber, st_l1.genericnumber2,st_l1.compensationdate
) A) lastest_premium
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
					AND st.compensationdate < to_date('06/30/2022','mm/dd/yyyy')
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




        

select * from
		cs_transactionassignment tgt,
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
					(SELECT st_sub.genericnumber2 FROM
					(select max(st_l1.sublinenumber) as sublinenumber, st_l1.alternateordernumber, st_l1.compensationdate from cs_salestransaction st_l1 ,(
select distinct st_in.alternateordernumber, st_in.compensationdate, sta_in.positionname FROM
					cs_salestransaction st_in
				INNER JOIN cs_transactionassignment sta_in ON
					sta_in.salestransactionseq = st_in.salestransactionseq
					AND sta_in.compensationdate = st_in.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st_in.eventtypeseq
					AND et.removedate = to_date('01/01/2200','mm/dd/yyyy')
				WHERE
					st_in.genericdate3 IS NULL
                    AND sta_in.processingunitseq= 38280596832649318
					AND st_in.genericnumber1 > st_in.genericnumber2 
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st_in.compensationdate >= to_date('06/01/2022','mm/dd/yyyy')
					AND st_in.compensationdate < to_date('06/30/2022','mm/dd/yyyy')) decr_txn, 
					cs_transactionassignment ta_l1, cs_eventtype et_l1
					where st_l1.compensationdate < decr_txn.compensationdate
					and st_l1.alternateordernumber = decr_txn.alternateordernumber
					and ta_l1.positionname=decr_txn.positionname
					and ta_l1.salestransactionseq=st_l1.salestransactionseq
					and st_l1.eventtypeseq=et_l1.datatypeseq
					and  et_l1.eventtypeid = 'SC-DK-001-001-SUMMARY'
					and et_l1.removedate = to_date('01/01/2200','mm/dd/yyyy')
					and st_l1.genericdate3 is  null
					and st_l1.alternateordernumber = '6530000910203'
				
					group by  st_l1.alternateordernumber,st_l1.compensationdate
) max_sub

inner join cs_salestransaction st_sub
st_sub.compensationdate=MAX_SUB.compensationdate 
										 AND MAX_SUB.alternateordernumber = st_sub.alternateordernumber
										 AND MAX_SUB.sublinenumber=st_sub.sublinenumber
										 INNER JOIN cs_eventtype et_sub ON
											et_sub.datatypeseq = st_sub.eventtypeseq
											AND et_sub.removedate = to_date('01/01/2200','mm/dd/yyyy') 
											where et_sub.eventtypeid = 'SC-DK-001-001-SUMMARY' ) lastest_premium
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
					AND st.compensationdate < to_date('06/30/2022','mm/dd/yyyy')
--					AND st.genericattribute10 IS null
					AND alternateordernumber in (6530000910203
)
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
		) src where 
			src.cantxns_salestransactionseq = tgt.SALESTRANSACTIONSEQ
			AND src.cantxns_positionname = tgt.positionname;
		
			-- select * from cs_period where name like '%Jun%2022%';
			



select st_sub.* from (	
					select  ROW_NUMBER() OVER ( partition by st_l1.alternateordernumber ORDER BY st_l1.compensationdate desc) row_num, st_l1.sublinenumber, 
					st_l1.alternateordernumber, st_l1.compensationdate from cs_salestransaction st_l1 ,(
select distinct st_in.alternateordernumber, st_in.compensationdate, sta_in.positionname FROM
					cs_salestransaction st_in
				INNER JOIN cs_transactionassignment sta_in ON
					sta_in.salestransactionseq = st_in.salestransactionseq
					AND sta_in.compensationdate = st_in.compensationdate
				INNER JOIN cs_eventtype et ON
					et.datatypeseq = st_in.eventtypeseq
					AND et.removedate = to_date('01/01/2200','mm/dd/yyyy')
				WHERE
					st_in.genericdate3 IS NULL
                    AND sta_in.processingunitseq= 38280596832649318
					AND st_in.genericnumber1 > st_in.genericnumber2 
					AND et.eventtypeid = 'SC-DK-001-001-SUMMARY'
					AND st_in.compensationdate >= to_date('06/01/2022','mm/dd/yyyy')
					AND st_in.compensationdate < to_date('06/30/2022','mm/dd/yyyy')) decr_txn, 
					cs_transactionassignment ta_l1, cs_eventtype et_l1
					where st_l1.compensationdate < decr_txn.compensationdate
					and st_l1.alternateordernumber = decr_txn.alternateordernumber
					and ta_l1.positionname=decr_txn.positionname
					and ta_l1.salestransactionseq=st_l1.salestransactionseq
					and st_l1.eventtypeseq=et_l1.datatypeseq
					and  et_l1.eventtypeid = 'SC-DK-001-001-SUMMARY'
					and et_l1.removedate = to_date('01/01/2200','mm/dd/yyyy')
					and st_l1.genericdate3 is  null
					and st_l1.alternateordernumber = '6530000910203'
				
					group by  st_l1.alternateordernumber,st_l1.sublinenumber, st_l1.compensationdate order by st_l1.compensationdate desc
) max_sub

inner join cs_salestransaction st_sub on
st_sub.compensationdate=MAX_SUB.compensationdate 
AND MAX_SUB.alternateordernumber = st_sub.alternateordernumber
AND MAX_SUB.sublinenumber=st_sub.sublinenumber
		INNER JOIN cs_eventtype et_sub ON
		et_sub.datatypeseq = st_sub.eventtypeseq
		AND et_sub.removedate = to_date('01/01/2200','mm/dd/yyyy') 
											where et_sub.eventtypeid = 'SC-DK-001-001-SUMMARY'
											and MAX_SUB.row_num=1