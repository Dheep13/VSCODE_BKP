CREATE OR REPLACE LIBRARY "EXT"."KYN_LIB_TQ2COM" LANGUAGE SQLSCRIPT AS
BEGIN
  PUBLIC variable rowcount int;
  PUBLIC variable v_terrProgram varchar(100);
  PUBLIC variable v_batchName varchar(100);
  PUBLIC variable v_pipelineRunSeq bigint;
  PUBLIC variable v_procName varchar(100);
  PUBLIC variable v_removeDate constant DATE := TO_DATE('01/01/2200','mm/dd/yyyy');
  PUBLIC VARIABLE v_sqlCount INT;
  PUBLIC VARIABLE v_tenantId varchar2(4);
  PUBLIC function get_element_type(i_targettype varchar(255)) returns v_ret varchar(255) as
  begin
    if :i_targettype = 'GP' then
      v_ret := 'Profile';
    else
      v_ret := :i_targettype;
    end if;
  end;
  PUBLIC procedure full_reset() as
  begin
    declare cursor c_tabs for select table_name, schema_name from tables where table_name like 'KYN$_TQ2COM$_%' escape '$' 
	and schema_name = 'EXT';
	for x as c_tabs
	do
	  execute immediate 'delete from '||x.schema_name||'.'||x.table_name;
	end for;
	commit;
  end;
  PUBLIC function get_min_quota(IN i_elementName VARCHAR(255),
    IN i_countryName VARCHAR(255),
    IN i_periodCycle INTEGER) returns o_minvalue integer as
  begin

    SELECT mc.value INTO o_minvalue
    -- , 'Q_' || :i_elementName || '_Minimum' 
    -- INTO o_min_value
    -- , o_quota_name
    FROM (SELECT country.NAME AS country_name, country.DIM1index AS DIM1INDEX, mi.minstring AS element_name, mc.mdltcellseq AS mdltcellseq, mc.DIM2index AS DIM2INDEX
        FROM (SELECT mi.minstring AS NAME, mc.mdltcellseq, mc.DIM1index FROM cs_mdltcell mc
            INNER JOIN cs_relationalmdlt rm ON mc.mdltseq = rm.ruleelementseq
            INNER JOIN cs_mdltindex mi ON mi.ordinal = mc.dim0index AND mi.ruleelementseq = rm.ruleelementseq AND mi.ruleelementseq = mc.mdltseq
            INNER JOIN cs_mdltdimension md ON md.dimensionseq = mi.dimensionseq AND md.ruleelementseq = rm.ruleelementseq AND md.ruleelementseq = mi.ruleelementseq
            WHERE rm.name = 'LT_Minimum_Quota'
                AND md.name = 'Country'
                AND mi.minstring = :i_countryName
                AND rm.removedate = :v_removeDate
                AND mc.removedate = :v_removeDate 
                AND mi.removedate = :v_removeDate 
                AND md.removedate = :v_removeDate
        ) country
        INNER JOIN cs_mdltcell mc ON mc.mdltcellseq = country.mdltcellseq
        INNER JOIN cs_relationalmdlt rm ON mc.mdltseq = rm.ruleelementseq
        INNER JOIN cs_mdltindex mi ON mi.ordinal = mc.dim1index AND mi.ruleelementseq = rm.ruleelementseq AND mi.ruleelementseq = mc.mdltseq
        INNER JOIN cs_mdltdimension md ON md.dimensionseq = mi.dimensionseq AND md.ruleelementseq = rm.ruleelementseq AND md.ruleelementseq = mi.ruleelementseq
        WHERE rm.name = 'LT_Minimum_Quota'
            AND md.name = 'Element'
            AND mi.minstring = :i_elementName
            AND rm.removedate = :v_removeDate
            AND mc.removedate = :v_removeDate 
            AND mi.removedate = :v_removeDate 
            AND md.removedate = :v_removeDate 
    ) country_element
    INNER JOIN cs_mdltcell mc ON mc.mdltcellseq = country_element.mdltcellseq
    INNER JOIN cs_relationalmdlt rm ON mc.mdltseq = rm.ruleelementseq
    INNER JOIN cs_mdltindex mi ON mi.ordinal = mc.dim2index AND mi.ruleelementseq = rm.ruleelementseq AND mi.ruleelementseq = mc.mdltseq
    INNER JOIN cs_mdltdimension md ON md.dimensionseq = mi.dimensionseq AND md.ruleelementseq = rm.ruleelementseq AND md.ruleelementseq = mi.ruleelementseq
    WHERE rm.name = 'LT_Minimum_Quota'
        AND md.name = 'Period Cycle'
        AND mi.minvalue = :i_periodCycle
        AND rm.removedate = :v_removeDate
        AND mc.removedate = :v_removeDate 
        AND mi.removedate = :v_removeDate 
        AND md.removedate = :v_removeDate;
  end;
  PUBLIC function get_sub_ordinates_count(IN i_positionSeq bigint, IN i_effectivestartdate timestamp, IN i_effectiveenddate timestamp)
  returns o_subordinates_count number as
  begin

   select count(1) into o_subordinates_count from cs_position where managerseq=:i_positionSeq
   and removedate = :v_removeDate and i_effectivestartdate between effectivestartdate and effectiveenddate
    and i_effectiveenddate between effectivestartdate and effectiveenddate;
  end;
  PUBLIC procedure get_territoryprogram_data(
             in pTerritoryProgram  varchar(100)
	
)
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin

DECLARE EXIT HANDLER FOR SQLEXCEPTION
    	BEGIN
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
 			resignal;
    	END;

v_procName = 'get_territoryprogram_data';
v_terrProgram = pTerritoryProgram;


--ex_employees = select * from "OPENSAP_MD_EMPLOYEES";
insert into ext.kyn_tq2com_sync (
territoryprogramseq, 
territoryprogram_name,
territoryprogram_esd,
territoryprogram_eed,
territoryprogram_periodtype,
territoryprogram_calendar,
territoryprogram_periodseq,
territoryprogram_period)
select 
tp.territoryprogramseq, 
tp.name, 
cast(tp.effectivestartdate as date) as esd,
cast(tp.effectiveenddate as date) as eed,
pt.name as periodtype,
cal_tp.name as calendar,
per_tp.periodseq as periodseq,
per_tp.name as period
from csq_territoryprogram tp
join cs_periodtype pt on tp.periodtypeseq = pt.periodtypeseq and pt.removedate= :v_removeDate
join cs_period per_tp on tp.periodseq = per_tp.periodseq and per_tp.removedate= :v_removeDate
join cs_calendar cal_tp on per_tp.calendarseq = cal_tp.calendarseq and cal_tp.removedate = :v_removeDate
where tp.removedate = :v_removeDate
and tp.name = :v_terrProgram;

v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_sync table load complete ',v_sqlCount);	


-- set semi annual details and batch
update ext.kyn_tq2com_sync x
set (x.semiannual_periodseq, x.semiannual_name) = (
	select y.periodseq, y.name from cs_period y
	where y.removedate = :v_removeDate
	and y.parentseq = x.territoryprogram_periodseq
	and ((y.name like 'HY1%' and x.territoryprogram_name like 'FY__H1%')
	or (y.name like 'HY2%' and x.territoryprogram_name like 'FY__H2%'))
);
 
v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> Updated kyn_tq2com_sync table with semi annual details ',:v_sqlCount);	
COMMIT;

-- rowcount = record_count(:ex_employees);
end;
  PUBLIC procedure get_tq_from_program( )
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin

DECLARE EXIT HANDLER FOR SQLEXCEPTION
begin
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
        resignal;
    	END;
    	
v_procName = 'get_tq_from_program';

INSERT INTO ext.kyn_tq2com_tq_quota  
(select
  jc.run_key,
  t.name as territory_name,
  cast(t.effectivestartdate as date) as territory_esd,
  cast(t.effectiveenddate as date) as territory_eed,  
  tt.targettypeid,
  cast(tq.effectivestartdate as date) as quota_esd,
  cast(tq.effectiveenddate as date) as quota_eed,  
  tq.quotavalue,
  ut_tq.name as unittype,
  tq.finalquotavalue,
  tq.casestatus as quota_casestatus,
  cast(tpos.effectivestartdate as date) as tpos_esd,
  cast(tpos.effectiveenddate as date) as tpos_eed,
  tpos.split,
  tpos.positionseq,
  pos.name as position,
  pos.payeeseq,
  pay.payeeid
from ext.kyn_tq2com_sync jc
join csq_territory t on 
  jc.territoryprogramseq = t.territoryprogramseq
  and t.removedate = :v_removeDate
  and t.effectivestartdate < jc.territoryprogram_eed
  and t.effectiveenddate > jc.territoryprogram_esd
join csq_territoryquota tq on 
  tq.territoryseq = t.territoryseq 
  and tq.removedate= :v_removeDate
  and tq.effectivestartdate < t.effectiveenddate
  and tq.effectiveenddate > t.effectivestartdate 
join csq_targettype tt on tq.targettypeseq = tt.datatypeseq and tt.removedate= :v_removeDate
join cs_unittype ut_tq on tq.unittypeforquotavalue = ut_tq.unittypeseq and ut_tq.removedate = :v_removeDate
join csq_territoryposition tpos on 
  tpos.territoryseq = t.territoryseq 
  and tpos.removedate = :v_removeDate
  and tpos.effectivestartdate < t.effectiveenddate
  and tpos.effectiveenddate > t.effectivestartdate 
join cs_position pos on 
  tpos.positionseq = pos.ruleelementownerseq 
  and pos.removedate = :v_removeDate
  and pos.effectivestartdate < tpos.effectiveenddate
  and pos.effectiveenddate >= tpos.effectiveenddate
join cs_payee pay on
  pos.payeeseq = pay.payeeseq
  and pay.removedate = :v_removeDate
  and pay.effectivestartdate < pos.effectiveenddate
  and pay.effectiveenddate >= pos.effectiveenddate
where jc.process_flag = 0
);
----Will have to apply KYN_TQ2COMM_Filter ---this will be based on run_key

v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_tq_quota table load with TQ quotas complete',v_sqlCount);	
COMMIT;

end;
  PUBLIC PROCEDURE TQ_PRESTAGE_VERSIONING (
    IN p_id INTEGER,
    IN p_data NVARCHAR(255),
    IN p_effective_date DATE
)

   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER

AS
    v_previous_end_date DATE;
BEGIN

	DECLARE v_max_run_key NUMBER; -- To count the number of inserted/deleted/updated rows. Used for the log procedure
	DECLARE v_second_max_run_key NUMBER;
	DECLARE v_end_month DATE;
	DECLARE proc_name VARCHAR2(50) := 'insert_versioned_record'; -- The name of the procedure. Used for the log procedure
	DECLARE tenant VARCHAR2(50);
	DECLARE v_removeDate DATE := TO_DATE('22000101','yyyymmdd');
	
	DECLARE v_maxperiodseq NUMBER;

SELECT MAX(run_key) into v_max_run_key
FROM ext.kyn_tq2com_tq_quota;

SELECT MAX(run_key) into v_second_max_run_key
FROM ext.kyn_tq2com_tq_quota
WHERE run_key < :v_max_run_key;


    -- -- Find the end date of the previous record
  
    -- -- Update the end date of the previous record
    -- UPDATE versioned_records
    -- SET end_date = ADD_DAYS(:p_effective_date, -1)
    -- WHERE id = :p_id AND effective_date = :v_previous_end_date;

    -- -- Insert the new record
    -- INSERT INTO versioned_records (id, data, effective_date, end_date)
    -- VALUES (:p_id, :p_data, :p_effective_date, '9999-12-31');


INSERT INTO ext.kyn_tq2com_tq_quota  
(select distinct
  jc.run_key,
  t.name as territory_name,
  cast(t.effectivestartdate as date) as territory_esd,
  cast(t.effectiveenddate as date) as territory_eed ,  
  tt.targettypeid,
  cast(tq.effectivestartdate as date) as quota_esd,
  cast(tq.effectiveenddate as date) as quota_eed,  
  tq.quotavalue,
  ut_tq.name as unittype,
  tq.finalquotavalue,
  tq.casestatus as quota_casestatus,
  cast(tpos.effectivestartdate as date) as tpos_esd,
  cast(tpos.effectiveenddate as date) as tpos_eed,
  tpos.split,
  tpos.positionseq,
  pos.name as position,
  pos.payeeseq,
  pay.payeeid
from ext.kyn_tq2com_sync jc
join csq_territory t on 
  jc.territoryprogramseq = t.territoryprogramseq
  and t.removedate = :v_removeDate
  and t.effectivestartdate < jc.territoryprogram_eed
  and t.effectiveenddate > jc.territoryprogram_esd
join csq_territoryquota tq on 
  tq.territoryseq = t.territoryseq 
  and tq.removedate= :v_removeDate
  and tq.effectivestartdate < t.effectiveenddate
  and tq.effectiveenddate > t.effectivestartdate 
join csq_targettype tt on tq.targettypeseq = tt.datatypeseq and tt.removedate= :v_removeDate
join cs_unittype ut_tq on tq.unittypeforquotavalue = ut_tq.unittypeseq and ut_tq.removedate = :v_removeDate
join csq_territoryposition tpos on 
  tpos.territoryseq = t.territoryseq 
  and tpos.removedate = :v_removeDate
  and tpos.effectivestartdate < t.effectiveenddate
  and tpos.effectiveenddate > t.effectivestartdate 
join cs_position pos on 
  tpos.positionseq = pos.ruleelementownerseq 
  and pos.removedate = :v_removeDate
  and pos.effectivestartdate < tpos.effectiveenddate
  and pos.effectiveenddate >= tpos.effectiveenddate
join cs_payee pay on
  pos.payeeseq = pay.payeeseq
  and pay.removedate = :v_removeDate
  and pay.effectivestartdate < pos.effectiveenddate
  and pay.effectiveenddate >= pos.effectiveenddate
where jc.process_flag = 0
);

   
MERGE INTO 
ext.kyn_tq2com_tq_quota PREV_REC
USING

   ( SELECT QI.RUN_KEY AS RUN_KEY,QI.TERRITORY_NAME, QI.TARGETTYPEID,QI.POSITION, COALESCE(MAX(QO.QUOTA_EED),'1900-01-01') AS QUOTA_EED, 
    COALESCE(MAX(QO.TPOS_EED),'1900-01-01') AS TPOS_EED, COALESCE(MAX(QO.TERRITORY_EED), '1900-01-01')  AS TERRITORY_EED,
    COALESCE(MAX(QI.QUOTA_ESD),'1900-01-01') AS QUOTA_ESD, COALESCE(MAX(QI.TPOS_ESD),'1900-01-01') AS TPOS_ESD,
    COALESCE(MAX(QI.TERRITORY_ESD),'1900-01-01') AS TERRITORY_ESD
    -- INTO v_previous_TERRITORY_EED
    -- INTO v_previous_QUOTA_EED
    -- INTO v_previous_TPOS_EED
    FROM ext.kyn_tq2com_tq_quota QO
    INNER JOIN ext.kyn_tq2com_tq_quota QI
    ON
    QI.TERRITORY_NAME=QO.TERRITORY_NAME
    AND QI.TARGETTYPEID = QO.TARGETTYPEID 
    AND QI.POSITION=QO.POSITION 
    AND QI.RUN_KEY=:v_max_run_key
    
    WHERE QO.run_key = :v_second_max_run_key
    group by QI.TERRITORY_NAME, QI.TARGETTYPEID, QI.POSITION,QI.run_key )
    NEW_REC
    ON
    (
    	PREV_REC.TERRITORY_NAME=NEW_REC.TERRITORY_NAME
    	AND PREV_REC.TARGETTYPEID=NEW_REC.TARGETTYPEID
    	AND PREV_REC.POSITION = NEW_REC.POSITION
    	AND PREV_REC.RUN_KEY=:v_second_max_run_key
    	AND NEW_REC.RUN_KEY=:v_max_run_key
    	AND PREV_REC.QUOTA_EED > NEW_REC.QUOTA_ESD
		AND	PREV_REC.TPOS_EED > NEW_REC.TPOS_ESD 
		AND	PREV_REC.TERRITORY_EED > NEW_REC.TERRITORY_ESD
    	
    )
    WHEN MATCHED THEN
		UPDATE SET 
			PREV_REC.QUOTA_EED = NEW_REC.QUOTA_ESD,
		    PREV_REC.TPOS_EED = NEW_REC.TPOS_ESD ,
			PREV_REC.TERRITORY_EED = NEW_REC.TERRITORY_ESD
    ;

    -- Commit the changes
    COMMIT;
    
END;
  PUBLIC procedure load_quota_to_prestage( )
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin

DECLARE v_error_code varchar(100);
DECLARE v_error_message varchar(4999);

DECLARE EXIT HANDLER FOR SQLEXCEPTION
    	BEGIN
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
            resignal;
    	END;
v_procName = 'load_quota_to_prestage';

insert into ext.kyn_tq2com_prestage_quota 
select * from (
select
qs.run_key,
qs.semiannual_periodseq,
qs.semiannual_name,
cast(0 as tinyint) as active_flag,
cast(null as timestamp) as active_start,
cast(null as timestamp) as active_end,
per.startdate as effectivestartdate,
per.enddate as effectiveenddate,
'Q_'||case when jc.targettypeid = 'GP' then 'Profit' else jc.targettypeid end||'_Original' as quotaname,
sum(jc.finalquotavalue) as value,
jc.unittype as unittypeforvalue,
pt.name as periodtypename,
null as businessunitmap,
jc.positionseq,
jc.position as positionname,
null as batchname
from ext.kyn_tq2com_tq_quota jc
join ext.kyn_tq2com_sync qs on jc.run_key = qs.run_key
join cs_period per on qs.semiannual_periodseq = per.periodseq and per.removedate = :v_removeDate
join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = :v_removeDate
where qs.process_flag = 0
group by qs.run_key, qs.semiannual_periodseq, qs.semiannual_name, per.startdate, per.enddate, jc.targettypeid, jc.unittype, pt.name, jc.positionseq, jc.position
);
-- x
-- where exists (select 1 from cs_quota q where q.removedate =:v_removeDate and x.quotaname = q.name));

v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_prestage_quota table load with original quotas for IPL generation complete',v_sqlCount);

INSERT INTO ext.kyn_tq2com_prestage_quota
select B.RUN_KEY, B.SEMIANNUAL_PERIODSEQ, B.SEMIANNUAL_NAME,
B.ACTIVE_FLAG,B.ACTIVE_START, B.ACTIVE_END,
B.EFFECTIVESTARTDATE, B.EFFECTIVEENDDATE,'Q_'||B.ELEMENTTYPE||'_Minimum', 
ext.get_min_quota(B.ELEMENTTYPE, B.COUNTRYCODE, B.PERIODCYCLE)
* greatest(ext.get_sub_ordinates_count(B.POSITIONSEQ, B.EFFECTIVESTARTDATE, B.EFFECTIVEENDDATE),1) AS MIN_QUOTA,
B.UNITTYPEFORVALUE,B.PERIODTYPENAME,
B.BUSINESSUNITMAP, 
B.POSITIONSEQ, B.POSITIONNAME, B.BATCHNAME
from (
select A.ELEMENTTYPE, A.position_ESD, A.position_eed , IFNULL(A.COUNTRYCODE,'AE') as COUNTRYCODE,
CASE WHEN SUBSTR(pl.name, INSTR(pl.name, '_', -1) + 1) = 6 THEN 1 ELSE 2 END AS PERIODCYCLE,
A.RUN_KEY, A.SEMIANNUAL_PERIODSEQ, A.SEMIANNUAL_NAME,
A.ACTIVE_FLAG,A.ACTIVE_START, A.ACTIVE_END,
A.EFFECTIVESTARTDATE, A.EFFECTIVEENDDATE,
A.UNITTYPEFORVALUE,A.PERIODTYPENAME,
A.BUSINESSUNITMAP, 
A.POSITIONSEQ, A.POSITIONNAME, A.BATCHNAME
from (
select po.name as positionname, case when tq.targettypeid = 'GP' then 'Profit' else tq.targettypeid end as ELEMENTTYPE,
po.titleseq, po.effectivestartdate as position_ESD, 
po.effectiveenddate as position_eed,
pa.genericattribute1 as COUNTRYCODE,
tqs.RUN_KEY, tqs.SEMIANNUAL_PERIODSEQ, tqs.SEMIANNUAL_NAME,
0 as ACTIVE_FLAG, null as ACTIVE_START, null as ACTIVE_END,
per.startdate as EFFECTIVESTARTDATE, per.enddate as EFFECTIVEENDDATE,
tq.unittype as UNITTYPEFORVALUE,
pt.name as PERIODTYPENAME,
null as BUSINESSUNITMAP, 
tQ.POSITIONSEQ, 
null as BATCHNAME
from 
ext.kyn_tq2com_sync tqs
join ext.kyn_tq2com_tq_quota tq on tq.run_key=tqs.run_key
join cs_period per on tqs.semiannual_periodseq = per.periodseq and per.removedate = :v_removeDate
join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = :v_removeDate
join cs_position po on 
po.ruleelementownerseq=tq.positionseq
and po.removedate= :v_removeDate
and po.effectivestartdate < per.enddate and po.effectiveenddate >= per.enddate
left outer join cs_participant pa on
pa.payeeseq=po.payeeseq
and pa.effectivestartdate < per.enddate and pa.effectiveenddate >= per.enddate
and pa.removedate= :v_removeDate
where tqs.process_flag = 0
) A
inner join CS_title ti 
on ti.ruleelementownerseq=A.titleseq
and ti.removedate = :v_removeDate
and ti.effectivestartdate < a.EFFECTIVEENDDATE
and ti.effectiveenddate >= a.EFFECTIVEENDDATE
left outer join cs_planassignable pa
on pa.ruleelementownerseq=ti.ruleelementownerseq
and pa.removedate = :v_removeDate
and pa.effectivestartdate < a.EFFECTIVEENDDATE
and pa.effectiveenddate >= a.EFFECTIVEENDDATE
left outer join cs_plan pl
on pl.ruleelementownerseq=pa.planseq
and pl.removedate = :v_removeDate
and pl.effectivestartdate < a.EFFECTIVEENDDATE
and pl.effectiveenddate >= a.EFFECTIVEENDDATE
) B;


v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_prestage_quota table load with minimum quotas for IPL generation complete',:v_sqlCount);	


INSERT INTO ext.kyn_tq2com_prestage_quota
select MINI.RUN_KEY, MINI.SEMIANNUAL_PERIODSEQ, MINI.SEMIANNUAL_NAME,
MINI.ACTIVE_FLAG,MINI.ACTIVE_START, MINI.ACTIVE_END,
MINI.EFFECTIVESTARTDATE,MINI.EFFECTIVEENDDATE,'Q_'||SUBSTR_BEFORE(SUBSTR_AFTER (ORG.quotaname,'_'),'_')||'_Final' as QUOTANAME, 
CASE WHEN ABS(MINI.VALUE) > ABS(ORG.VALUE) THEN ABS(MINI.VALUE) ELSE ABS(ORG.VALUE) END AS FINAL_VALUE, 
MINI.UNITTYPEFORVALUE,MINI.PERIODTYPENAME,
MINI.BUSINESSUNITMAP, 
MINI.POSITIONSEQ,MINI.POSITIONNAME,MINI.BATCHNAME
FROM 
ext.kyn_tq2com_sync tqs
join ext.kyn_tq2com_prestage_quota ORG on tqs.run_key = org.run_key
INNER JOIN ext.kyn_tq2com_prestage_quota MINI
ON MINI.RUN_KEY=ORG.RUN_KEY
-- and MINI.quotaname like '%Minimum%'
-- and ORG.quotaname like '%Original%'
AND MINI.SEMIANNUAL_PERIODSEQ=ORG.SEMIANNUAL_PERIODSEQ
AND MINI.SEMIANNUAL_NAME=ORG.SEMIANNUAL_NAME
AND MINI.ACTIVE_FLAG=ORG.ACTIVE_FLAG
AND IFNULL(MINI.ACTIVE_START,current_date)= IFNULL(ORG.ACTIVE_START ,current_date)
AND IFNULL( MINI.ACTIVE_END,current_date)= IFNULL(ORG.ACTIVE_END,current_date)
AND MINI.EFFECTIVESTARTDATE=ORG.EFFECTIVESTARTDATE
AND MINI.EFFECTIVEENDDATE=ORG.EFFECTIVEENDDATE
AND SUBSTR_BEFORE(SUBSTR_AFTER (ORG.quotaname,'_'),'_') = SUBSTR_BEFORE(SUBSTR_AFTER (MINI.quotaname,'_'),'_')
AND MINI.UNITTYPEFORVALUE=ORG.UNITTYPEFORVALUE
AND MINI.PERIODTYPENAME=ORG.PERIODTYPENAME
AND IFNULL(MINI.BUSINESSUNITMAP,1)=IFNULL(ORG.BUSINESSUNITMAP,1)
AND MINI.POSITIONSEQ=ORG.POSITIONSEQ
AND MINI.POSITIONNAME=ORG.POSITIONNAME
where MINI.quotaname like '%Minimum%'
and ORG.quotaname like '%Original%';

v_sqlCount = ::ROWCOUNT;	

update ext.kyn_tq2com_prestage_quota set quotaname = replace(quotaname, '_GP_','_Profit_') 
where run_key in (select run_key from ext.kyn_tq2com_sync where process_flag = 0);

CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_prestage_quota table load with final quotas for IPL generation complete',:v_sqlCount);	
COMMIT;



end;
  PUBLIC procedure get_account_and_product_from_tq( )
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin

DECLARE v_max_run_key number;
DECLARE v_second_max_run_key number;
DECLARE EXIT HANDLER FOR SQLEXCEPTION
    	BEGIN
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
            resignal;
    	END;

v_procName = 'get_account_and_product_from_tq';

insert into ext.kyn_tq2com_account 
(select
  qs.run_key,
  qs.semiannual_periodseq,
  qs.semiannual_name,
  cast(0 as tinyint) as active_flag,
  cast(null as timestamp) as active_start,
  cast(null as timestamp) as active_end,
  t.name as territory,
  cast(t.effectivestartdate as date) t_esd,
  cast(t.effectiveenddate as date) t_eed,
  tpos.positionseq,
  pos.name as position,
  cast(pos.effectivestartdate as date) pos_esd,
  cast(pos.effectiveenddate as date) pos_eed,
  cast(tacc.effectivestartdate as date) tacc_esd,
  cast(tacc.effectiveenddate as date) tacc_edd,
  acc.accountid,
  tacc.isaddedduetoparent
from ext.kyn_tq2com_sync qs
join csq_territory t on 
  qs.territoryprogramseq = t.territoryprogramseq
  and t.removedate = :v_removeDate
  and t.effectivestartdate < qs.territoryprogram_eed
  and t.effectiveenddate > qs.territoryprogram_esd
join csq_territoryaccount tacc on
  tacc.territoryseq = t.territoryseq
  and tacc.removedate = :v_removeDate
  and tacc.effectivestartdate < t.effectiveenddate
  and tacc.effectiveenddate > t.effectivestartdate  
join csq_account acc on
  acc.accountseq = tacc.accountseq
  and acc.removedate = :v_removeDate
  and acc.effectivestartdate < tacc.effectiveenddate
  and acc.effectiveenddate > tacc.effectivestartdate
join csq_territoryposition tpos on 
  tpos.territoryseq = t.territoryseq 
  and tpos.removedate = :v_removeDate
  and tpos.effectivestartdate < t.effectiveenddate
  and tpos.effectiveenddate > t.effectivestartdate
join cs_position pos on 
  tpos.positionseq = pos.ruleelementownerseq 
  and pos.removedate = :v_removeDate
  and pos.effectivestartdate < tpos.effectiveenddate
  and pos.effectiveenddate > tpos.effectivestartdate
where qs.process_flag = 0
);

v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_account table load with accounts information complete',v_sqlCount);	



insert into ext.kyn_tq2com_product (
select
qs.run_key,
qs.semiannual_periodseq,
qs.semiannual_name,
cast(0 as tinyint) as active_flag,
cast(null as timestamp) as active_start,
cast(null as timestamp) as active_end,
t.name as territory,
cast(t.effectivestartdate as date) t_esd, 
cast(t.effectiveenddate as date) t_eed,
tpos.positionseq,
pos.name as position,
cast(pos.effectivestartdate as date) pos_esd, 
cast(pos.effectiveenddate as date) pos_eed,
cast(tprd.effectivestartdate as date) tprd_esd, 
cast(tprd.effectiveenddate as date) tprd_edd,
cat.name as category,
prd.classifierid,
tprd.source,
cast(0 as tinyint) as process_flag
from ext.kyn_tq2com_sync qs
join csq_territory t on 
  qs.territoryprogramseq = t.territoryprogramseq
  and t.removedate = :v_removeDate
  and t.effectivestartdate < qs.territoryprogram_eed
  and t.effectiveenddate > qs.territoryprogram_esd
join csq_territoryproduct tprd on
  tprd.territoryseq = t.territoryseq
  and tprd.removedate = :v_removeDate
  and tprd.effectivestartdate < t.effectiveenddate
  and tprd.effectiveenddate > t.effectivestartdate      
join cs_classifier prd on 
  prd.classifierseq = tprd.productseq
  and prd.removedate = :v_removeDate
  and prd.effectivestartdate < tprd.effectiveenddate
  and prd.effectiveenddate > tprd.effectivestartdate   
left outer join cs_category cat on 
  tprd.categoryseq = cat.ruleelementseq
  and cat.removedate = :v_removeDate
  and cat.effectivestartdate < tprd.effectiveenddate
  and cat.effectiveenddate > tprd.effectivestartdate
join csq_territoryposition tpos on 
  tpos.territoryseq = t.territoryseq 
  and tpos.removedate = :v_removeDate
  and tpos.effectivestartdate < t.effectiveenddate
  and tpos.effectiveenddate > t.effectivestartdate
join cs_position pos on 
  tpos.positionseq = pos.ruleelementownerseq 
  and pos.removedate = :v_removeDate
  and pos.effectivestartdate < tpos.effectiveenddate
  and pos.effectiveenddate > tpos.effectivestartdate  
where qs.process_flag = 0);

v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_product table load with products information complete',:v_sqlCount);	
COMMIT;



end;
  PUBLIC procedure generate_ipl_trace_info( )
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin


DECLARE EXIT HANDLER FOR SQLEXCEPTION
    	BEGIN
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
            resignal;
    	END;

v_procName = 'generate_ipl_trace_info';
insert into ext.kyn_tq2com_ipl_trace (
select
dp.documentprocessseq, dp.generatedate, dp.name, dp.batchname, dp.startdate, dp.enddate,
dp.positionseq, pos.name as position, dp.status, dp.acceptdate,
per.periodseq as semiannual_periodseq,
per.name as semiannual_name,
cast(null as bigint) as run_key,
cast(0 as tinyint) as process_flag
from csp_documentprocess dp
join cs_position pos on 
  dp.positionseq = pos.ruleelementownerseq 
  and pos.removedate = :v_removeDate 
  and pos.effectivestartdate < dp.enddate 
  and pos.effectiveenddate >= dp.enddate
join cs_period per on
  per.startdate >= dp.startdate
  and per.enddate <= dp.enddate
  and per.removedate = :v_removeDate
join cs_periodtype pt on
  per.periodtypeseq = pt.periodtypeseq 
  and pt.removedate = :v_removeDate
  and pt.name = 'semiannual' 
where exists (
  -- must be for a plan document
  select 1
  from csp_documentassignment da
  join csp_documenttemplate dt on da.documenttemplateseq = dt.documenttemplateseq and dt.removedate = :v_removeDate
  join csp_documenttype dty on dt.documenttypeseq = dty.datatypeseq and dty.removedate = :v_removeDate
  where dp.documentprocesstemplateseq = da.documentprocesstemplateseq 
  and da.removedate = :v_removeDate
  and dty.datatype = 'Plan'
)
and not exists (select 1 from ext.kyn_tq2com_ipl_trace x where x.documentprocessseq = dp.documentprocessseq)
);

v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_ipl_trace table load with ipl trace info complete',:v_sqlCount);	

-- set run_key
update ext.kyn_tq2com_ipl_trace ipl
set ipl.run_key = (
select max(s.run_key)
from
ext.kyn_tq2com_sync s
join ext.kyn_tq2com_tq_quota q on s.run_key = q.run_key
where s.semiannual_periodseq = ipl.semiannual_periodseq
and q.positionseq = ipl.positionseq
and s.run_date <= ipl.generatedate
);


v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_ipl_trace table updated with run_key',:v_sqlCount);	


-- refresh status
update ext.kyn_tq2com_ipl_trace x
set (x.status, x.acceptdate) = (select y.status, y.acceptdate from csp_documentprocess y where x.documentprocessseq = y.documentprocessseq)
where process_flag = 0 and run_key is not null;


v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_ipl_trace table updated with status and accept date',:v_sqlCount);	


COMMIT;

end;
  PUBLIC procedure accept_ipl_quota( IN p_ipl_acceptdate timestamp, IN p_accepted_quota varchar(50), IN p_accepted_position varchar(100), IN p_accepted_semiannual_name varchar(100))
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin

DECLARE v_quota_exists number;
DECLARE v_procname varchar(50);
DECLARE v_removeDate constant DATE := TO_DATE('01/01/2200','mm/dd/yyyy');
DECLARE EXIT HANDLER FOR SQLEXCEPTION
    	BEGIN
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
            resignal;
    	END;

v_procName = 'accept_ipl_quota';



select count(1) into v_quota_exists
from  ext.KYN_TQ2COM_PRESTAGE_QUOTA
where ACTIVE_FLAG=1
and QUOTANAME= :p_accepted_quota 
and positionname=:p_accepted_position
and semiannual_name =:p_accepted_semiannual_name
and run_key < (SELECT MAX(run_key)
FROM ext.kyn_tq2com_tq_quota);

if :v_quota_exists > 0 then

update ext.KYN_TQ2COM_PRESTAGE_QUOTA
set ACTIVE_FLAG=0, ACTIVE_END = :p_ipl_acceptdate
where QUOTANAME= :p_accepted_quota and positionname=:p_accepted_position
and semiannual_name =:p_accepted_semiannual_name
and ACTIVE_FLAG=1
and run_key < (SELECT MAX(run_key)
FROM ext.kyn_tq2com_tq_quota);

end if;


update ext.KYN_TQ2COM_PRESTAGE_QUOTA
set ACTIVE_FLAG=1, ACTIVE_START = :p_ipl_acceptdate, ACTIVE_END = :v_RemoveDate
where ACTIVE_FLAG=0 and QUOTANAME= :p_accepted_quota and positionname=:p_accepted_position
and semiannual_name =:p_accepted_semiannual_name
and run_key=(SELECT MAX(run_key)
FROM ext.kyn_tq2com_tq_quota);


COMMIT;


END;
  PUBLIC procedure accept_ipl_account( IN p_ipl_acceptdate timestamp, IN p_accepted_account varchar(50), IN p_accepted_position varchar(100),
	IN p_accepted_semiannual_name varchar(50), IN p_accepted_territory varchar(100)
)
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin

DECLARE v_account_exists number;
DECLARE v_procname varchar(50);
DECLARE v_removeDate constant DATE := TO_DATE('01/01/2200','mm/dd/yyyy');
DECLARE EXIT HANDLER FOR SQLEXCEPTION
    	BEGIN
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
            resignal;
    	END;

v_procName = 'accept_ipl_account';

select * from ext.kyn_tq2com_account;

select count(1) into v_account_exists
from  ext.kyn_tq2com_account
where ACTIVE_FLAG=1
and ACCOUNTID= :p_accepted_account 
and position=:p_accepted_position
and semiannual_name =:p_accepted_semiannual_name
and territory =:p_accepted_territory
and run_key < (SELECT MAX(run_key)
FROM ext.kyn_tq2com_tq_quota);

if :v_account_exists > 0 then

update ext.kyn_tq2com_account
set ACTIVE_FLAG=0, ACTIVE_END = :p_ipl_acceptdate
where ACCOUNTID= :p_accepted_account and position=:p_accepted_position
and semiannual_name =:p_accepted_semiannual_name
AND ACCOUNTID= :p_accepted_account 
and ACTIVE_FLAG=1
and run_key < (SELECT MAX(run_key)
FROM ext.kyn_tq2com_tq_quota)
and territory=:p_accepted_territory ;

end if;


update ext.kyn_tq2com_account
set ACTIVE_FLAG=1, ACTIVE_START = :p_ipl_acceptdate, ACTIVE_END = :v_RemoveDate
where ACTIVE_FLAG=0 and ACCOUNTID= :p_accepted_account and position=:p_accepted_position
and semiannual_name =:p_accepted_semiannual_name
and territory=:p_accepted_territory 
and run_key=(SELECT MAX(run_key)
FROM ext.kyn_tq2com_tq_quota);


COMMIT;

END;
  PUBLIC procedure accept_ipl_product( IN p_ipl_acceptdate timestamp, IN p_accepted_product varchar(50), IN p_accepted_position varchar(100),
	IN p_accepted_semiannual_name varchar(50), IN p_accepted_territory varchar(100)
)
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin

DECLARE v_product_exists number;
DECLARE v_procname varchar(50);
DECLARE v_removeDate constant DATE := TO_DATE('01/01/2200','mm/dd/yyyy');
DECLARE EXIT HANDLER FOR SQLEXCEPTION
    	BEGIN
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
            resignal;
    	END;

v_procName = 'accept_ipl_product';

select * from ext.kyn_tq2com_product;

select count(1) into v_product_exists
from  ext.kyn_tq2com_product
where ACTIVE_FLAG=1
and CLASSIFIERID= :p_accepted_product 
and position=:p_accepted_position
and semiannual_name =:p_accepted_semiannual_name
and territory =:p_accepted_territory
and run_key < (SELECT MAX(run_key)
FROM ext.kyn_tq2com_tq_quota);

if :v_product_exists > 0 then

update ext.kyn_tq2com_product
set ACTIVE_FLAG=0, ACTIVE_END = :p_ipl_acceptdate
where CLASSIFIERID= :p_accepted_product and position=:p_accepted_position
and semiannual_name =:p_accepted_semiannual_name
AND CLASSIFIERID= :p_accepted_product 
and ACTIVE_FLAG=1
and run_key < (SELECT MAX(run_key)
FROM ext.kyn_tq2com_tq_quota)
and territory=:p_accepted_territory ;

end if;


update ext.kyn_tq2com_product
set ACTIVE_FLAG=1, ACTIVE_START = :p_ipl_acceptdate, ACTIVE_END = :v_RemoveDate
where ACTIVE_FLAG=0 and CLASSIFIERID= :p_accepted_product and position=:p_accepted_position
and semiannual_name =:p_accepted_semiannual_name
and territory=:p_accepted_territory 
and run_key=(SELECT MAX(run_key)
FROM ext.kyn_tq2com_tq_quota);

COMMIT;

END;
  PUBLIC procedure load_stagequota( )
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin
DECLARE v_tenantId varchar2(4);
DECLARE EXIT HANDLER FOR SQLEXCEPTION
    	BEGIN
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
            resignal;
    	END;

v_procName = 'load_stagequota';
-- load stagequota for accepted IPL

select tenantid into v_tenantId from cs_tenant;

insert into cs_stagequota
(tenantid, stagequotaseq, effectivestartdate, effectiveenddate, quotaname, value, unittypeforvalue, periodtypename, businessunitmap, positionname, batchname, stageprocessflag, description)
select :v_tenantId as tenantid,
(select max(stagequotaseq) from cs_stagequota) + row_number() over (order by quotaname, positionname) as stagequotaseq,
q.effectivestartdate, q.effectiveenddate, q.quotaname, q.value, 
q.unittypeforvalue, q.periodtypename, q.businessunitmap, q.positionname, 
'TQ2COM_'||TO_CHAR(current_timestamp,'YYYYMMDD_HH24MISS') as batchname, -- need to set the batchname
0 as stageprocessflag, 
null as description
from ext.kyn_tq2com_prestage_quota q
join ext.kyn_tq2com_ipl_trace ipl on q.run_key = ipl.run_key and q.positionseq = ipl.positionseq
where ipl.process_flag = 0
and ipl.status = 'status_Accepted';

v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> cs_stagequota table load with accepted IPL complete',:v_sqlCount);	
COMMIT;


end;
  PUBLIC procedure set_active_status( )
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin

DECLARE EXIT HANDLER FOR SQLEXCEPTION
    	BEGIN
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
            resignal;
    	END;

update ext.kyn_tq2com_prestage_quota q
set active_flag = 1, active_start = (
select acceptdate 
from ext.kyn_tq2com_ipl_trace ipl
where ipl.process_flag = 0
and ipl.status = 'status_Accepted'
and q.run_key = ipl.run_key 
and q.positionseq = ipl.positionseq
),
active_end = :v_removeDate
where exists (
select 1 
from ext.kyn_tq2com_ipl_trace ipl
where ipl.process_flag = 0
and ipl.status = 'status_Accepted'
and q.run_key = ipl.run_key 
and q.positionseq = ipl.positionseq
);

v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_prestage_quota table updated with accepted status',:v_sqlCount);	



update ext.kyn_tq2com_account q
set active_flag = 1, active_start = (
select acceptdate 
from ext.kyn_tq2com_ipl_trace ipl
where ipl.process_flag = 0
and ipl.status = 'status_Accepted'
and q.run_key = ipl.run_key 
and q.positionseq = ipl.positionseq
),
active_end = :v_removeDate
where exists (
select 1 
from ext.kyn_tq2com_ipl_trace ipl
where ipl.process_flag = 0
and ipl.status = 'status_Accepted'
and q.run_key = ipl.run_key 
and q.positionseq = ipl.positionseq
);

v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_account table updated with accepted status',:v_sqlCount);	


update ext.kyn_tq2com_product q
set active_flag = 1, active_start = (
select acceptdate 
from ext.kyn_tq2com_ipl_trace ipl
where ipl.process_flag = 0
and ipl.status = 'status_Accepted'
and q.run_key = ipl.run_key 
and q.positionseq = ipl.positionseq
),
active_end = :v_removeDate
where exists (
select 1 
from ext.kyn_tq2com_ipl_trace ipl
where ipl.process_flag = 0
and ipl.status = 'status_Accepted'
and q.run_key = ipl.run_key 
and q.positionseq = ipl.positionseq
);

v_sqlCount = ::ROWCOUNT;	
CALL EXT.kyn_prc_debug(:v_procName||'=> kyn_tq2com_product table updated with accepted status',:v_sqlCount);	
COMMIT;

end;
  PUBLIC procedure trigger_quota_import( )
   LANGUAGE SQLSCRIPT
   SQL SECURITY INVOKER
   AS
begin

   DECLARE v_qm varchar(30) = '?' ;
  --DECLARE o_pipelinerunseq BIGINT;
   --DECLARE v_pipelinerunseq BIGINT;
   DECLARE CURSOR c_cursor1 (v_batchName VARCHAR(100)) FOR
    select distinct batchname from cs_stagequota where batchname like 'TQ2COM_%' and stageprocessflag=0 and stageprocessdate is NULL
    order by batchname desc;

DECLARE EXIT HANDLER FOR SQLEXCEPTION
    	BEGIN
 			EXT.kyn_prc_debug (::SQL_ERROR_CODE, ::SQL_ERROR_MESSAGE);
            resignal;
    	END;

    FOR cur_row AS c_cursor1(v_batchName) 
	DO
        ext.kyn_lib_pipeline:v_and_t_quota(i_batchname => cur_row.batchname, o_pipelinerunseq => v_qm);
        -- v_pipelineRunSeq := o_pipelinerunseq;
        EXT.kyn_prc_debug ('Quota import triggered for batchname => '|| cur_row.batchname ||' with Pipelinerunseq => '|| v_qm);
        -- call ext.kyn_lib_pipeline:v_and_t_quota(i_batchname => 'Test', o_pipelinerunseq => ?);
    END FOR;

end;
  PUBLIC procedure update_sync() as
  begin
    update ext.kyn_tq2com_sync set process_flag = 3 where process_flag = 0;
  end;
  PUBLIC procedure run(IN i_territoryProgram nvarchar(500)) as
  begin

  DECLARE vflag INTEGER;
  DECLARE i INTEGER;
  DECLARE start_time TIMESTAMP := CURRENT_UTCTIMESTAMP;
  DECLARE end_time TIMESTAMP :=  ADD_SECONDS(start_time,10);

  select count(distinct batchname) into vflag from cs_stagequota where batchname like 'TQ2COM_%' 
  and stageprocessflag=0 and stageprocessdate is NULL;

WHILE vflag <> 0 DO

  WHILE CURRENT_UTCTIMESTAMP <= end_time DO
          i=i+1;
  END WHILE; 

  select count(distinct batchname) into vflag from cs_stagequota where batchname like 'TQ2COM_%' 
  and stageprocessflag=0 and stageprocessdate is NULL;

END WHILE;
  get_territoryprogram_data( pTerritoryProgram => :i_territoryProgram);
  get_tq_from_program();
  load_quota_to_prestage();
  get_account_and_product_from_tq();
  update_sync();
  generate_ipl_trace_info();
  load_stagequota();
  set_active_status();
  --trigger_quota_import();

  commit;

  end;
END