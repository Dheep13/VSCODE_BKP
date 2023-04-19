select * from ext.jc_quota_sync;

select * from cs_processingunit;

CREATE SEQUENCE KYN_TQ2COM_quota_sync_seq START WITH 1 INCREMENT BY 1;
create table KYN_TQ2COM_SYNC (
run_key bigint not null primary key generated by default as identity,
run_date timestamp default current_timestamp,
territoryprogramseq bigint not null,
territoryprogram_name varchar(127),
territoryprogram_esd date,
territoryprogram_eed date,
territoryprogram_periodtype varchar(50),
territoryprogram_calendar varchar(50),
territoryprogram_periodseq bigint,
territoryprogram_period varchar(50),
semiannual_periodseq bigint,
semiannual_name varchar(50),
stagequota_batchname varchar(255),
process_flag tinyint default 0 not null
);


-- workflow or trigger adds entries to tracking table
--incremental load? or pull data every period
--one time migration load?
INSERT INTO KYN_TQ2COM_SYNC 
(select 
KYN_TQ2COM_quota_sync_seq.NEXTVAL as run_key,
current_timestamp,
tp.territoryprogramseq, 
tp.name, 
cast(tp.effectivestartdate as date) as esd,
cast(tp.effectiveenddate as date) as eed,
pt.name as periodtype,
cal_tp.name as calendar,
per_tp.periodseq as periodseq,
per_tp.name as period,
per_tps.periodseq as semi_annual_periodseq,
per_tps.name as semi_annual_periodname,
'JC_TEST#'||to_char(current_timestamp, 'YYYYMMDD_HH24MISS') AS stagequota_batchname,
0
from csq_territoryprogram tp
left outer join cs_periodtype pt on tp.periodtypeseq = pt.periodtypeseq and pt.removedate= '2200-01-01' 
left outer join cs_period per_tp on tp.periodseq = per_tp.periodseq and per_tp.removedate= '2200-01-01'
left outer join cs_periodtype pts on tp.periodtypeseq = pts.periodtypeseq and pts.removedate= '2200-01-01' and pts.name='semiannual'
left outer join cs_period per_tps on tp.periodtypeseq = per_tps.periodtypeseq and pts.periodtypeseq=per_tps.periodseq and per_tps.removedate='2200-01-01'
left outer join cs_calendar cal_tp on per_tp.calendarseq = cal_tp.calendarseq and cal_tp.removedate = '2200-01-01'
where tp.removedate = '2200-01-01'
and tp.islast = 1);


update KYN_TQ2COM_SYNC x
set x.stagequota_batchname = x.stagequota_batchname || '#' || x.run_key, -- make this unique
(x.semiannual_periodseq, x.semiannual_name) = (
	select y.periodseq, y.name from cs_period y
	where y.removedate = '2200-01-01'
	and y.parentseq = x.territoryprogram_periodseq
	and ((y.name like 'HY1%' and x.territoryprogram_name like 'FY__H1%') or (y.name like 'HY2%' and x.territoryprogram_name like 'FY__H2%'))
);




-- create table KYN_TQ2COM_TQ_QUOTA as (
-- select
--   run_key,
-- /*  tp.name as program,
--   cast(tp.effectivestartdate as date) as program_esd,
--   cast(tp.effectiveenddate as date) as program_eed,
--   pt.name as program_periodtype,
--   cal_tp.name as program_calendar,
--   per_tp.periodseq as program_periodseq,
--   per_tp.name as program_period,*/
--   t.name as territory,
--   cast(t.effectivestartdate as date) as territory_esd,
--   cast(t.effectiveenddate as date) as territory_eed,  
--   tt.targettypeid,
--   cast(tq.effectivestartdate as date) as quota_esd,
--   cast(tq.effectiveenddate as date) as quota_eed,  
--   tq.quotavalue,
--   ut_tq.name as unittype,
--   tq.finalquotavalue,
--   tq.casestatus as quota_casestatus,
--   cast(tpos.effectivestartdate as date) as tpos_esd,
--   cast(tpos.effectiveenddate as date) as tpos_eed,
--   tpos.split,
--   tpos.positionseq,
--   pos.name as position,
--   pos.payeeseq,
--   pay.payeeid
-- from --CSQ_TerritoryProgram tp
-- --join 
-- KYN_TQ2COM_SYNC jc --on tp.territoryseq = jc.territoryprogramseq and jc.process_flag = 0
-- --left outer join cs_periodtype pt on tp.periodtypeseq = pt.periodtypeseq and pt.removedate= '2200-01-01'
-- --left outer join cs_period per_tp on tp.periodseq = per_tp.periodseq and per_tp.removedate= '2200-01-01'
-- --left outer join cs_calendar cal_tp on per_tp.calendarseq = cal_tp.calendarseq and cal_tp.removedate = '2200-01-01'
-- left outer join csq_territory t on 
--   jc.territoryprogramseq = t.territoryprogramseq
--   and t.removedate = '2200-01-01'
--   and t.effectivestartdate < jc.territoryprogram_eed --effectiveenddate
--   and t.effectiveenddate > jc.territoryprogram_esd -- jc.effectivestartdate 
-- left outer join csq_territoryquota tq on 
--   tq.territoryseq = t.territoryseq 
--   and tq.removedate= '2200-01-01' 
--   and tq.effectivestartdate < t.effectiveenddate
--   and tq.effectiveenddate > t.effectivestartdate 
-- left outer join csq_targettype tt on tq.targettypeseq = tt.datatypeseq and tt.removedate= '2200-01-01'
-- left outer join cs_unittype ut_tq on tq.unittypeforquotavalue = ut_tq.unittypeseq and ut_tq.removedate = '2200-01-01'
-- left outer join csq_territoryposition tpos on 
--   tpos.territoryseq = t.territoryseq 
--   and tpos.removedate = '2200-01-01' 
--   and tpos.effectivestartdate < t.effectiveenddate
--   and tpos.effectiveenddate > t.effectivestartdate 
-- left outer join cs_position pos on 
--   tpos.positionseq = pos.ruleelementownerseq 
--   and pos.removedate = '2200-01-01'
--   and pos.effectivestartdate < tpos.effectiveenddate
--   and pos.effectiveenddate >= tpos.effectiveenddate
-- left outer join cs_payee pay on
--   pos.payeeseq = pay.payeeseq
--   and pay.removedate = '2200-01-01'
--   and pay.effectivestartdate < pos.effectiveenddate
--   and pay.effectiveenddate >= pos.effectiveenddate
-- where jc.process_flag = 8
--  --tp.removedate = '2200-01-01'
-- );

insert into KYN_TQ2COM_TQ_QUOTA 
(
select
  run_key,
  t.name as territory,
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
from 
KYN_TQ2COM_SYNC jc 
left outer join csq_territory t on 
  jc.territoryprogramseq = t.territoryprogramseq
  and t.removedate = '2200-01-01'
  and t.effectivestartdate < jc.territoryprogram_eed 
  and t.effectiveenddate > jc.territoryprogram_esd 
left outer join csq_territoryquota tq on 
  tq.territoryseq = t.territoryseq 
  and tq.removedate= '2200-01-01' 
  and tq.effectivestartdate < t.effectiveenddate
  and tq.effectiveenddate > t.effectivestartdate 
left outer join csq_targettype tt on tq.targettypeseq = tt.datatypeseq and tt.removedate= '2200-01-01'
left outer join cs_unittype ut_tq on tq.unittypeforquotavalue = ut_tq.unittypeseq and ut_tq.removedate = '2200-01-01'
left outer join csq_territoryposition tpos on 
  tpos.territoryseq = t.territoryseq 
  and tpos.removedate = '2200-01-01' 
  and tpos.effectivestartdate < t.effectiveenddate
  and tpos.effectiveenddate > t.effectivestartdate 
left outer join cs_position pos on 
  tpos.positionseq = pos.ruleelementownerseq 
  and pos.removedate = '2200-01-01'
  and pos.effectivestartdate < tpos.effectiveenddate
  and pos.effectiveenddate >= tpos.effectiveenddate
left outer join cs_payee pay on
  pos.payeeseq = pay.payeeseq
  and pay.removedate = '2200-01-01'
  and pay.effectivestartdate < pos.effectiveenddate
  and pay.effectiveenddate >= pos.effectiveenddate
where jc.process_flag = 8
);


-- create table 
-- insert into KYN_TQ2COM_TQ_QUOTA 
-- (
-- select
--   run_key,
--   t.name as territory,
--   cast(t.effectivestartdate as date) as territory_esd,
--   cast(t.effectiveenddate as date) as territory_eed,  
--   tt.targettypeid,
--   cast(tq.effectivestartdate as date) as quota_esd,
--   cast(tq.effectiveenddate as date) as quota_eed,  
--   tq.quotavalue,
--   ut_tq.name as unittype,
--   tq.finalquotavalue,
--   tq.casestatus as quota_casestatus,
--   cast(tpos.effectivestartdate as date) as tpos_esd,
--   cast(tpos.effectiveenddate as date) as tpos_eed,
--   tpos.split,
--   tpos.positionseq,
--   pos.name as position,
--   pos.payeeseq,
--   pay.payeeid
-- from 
-- KYN_TQ2COM_SYNC jc 
-- left outer join csq_territory t on 
--   jc.territoryprogramseq = t.territoryprogramseq
--   and t.removedate = '2200-01-01'
--   and t.effectivestartdate < jc.territoryprogram_eed 
--   and t.effectiveenddate > jc.territoryprogram_esd 
-- left outer join csq_territoryquota tq on 
--   tq.territoryseq = t.territoryseq 
--   and tq.removedate= '2200-01-01' 
--   and tq.effectivestartdate < t.effectiveenddate
--   and tq.effectiveenddate > t.effectivestartdate 
-- left outer join csq_targettype tt on tq.targettypeseq = tt.datatypeseq and tt.removedate= '2200-01-01'
-- left outer join cs_unittype ut_tq on tq.unittypeforquotavalue = ut_tq.unittypeseq and ut_tq.removedate = '2200-01-01'
-- left outer join csq_territoryposition tpos on 
--   tpos.territoryseq = t.territoryseq 
--   and tpos.removedate = '2200-01-01' 
--   and tpos.effectivestartdate < t.effectiveenddate
--   and tpos.effectiveenddate > t.effectivestartdate 
-- left outer join cs_position pos on 
--   tpos.positionseq = pos.ruleelementownerseq 
--   and pos.removedate = '2200-01-01'
--   and pos.effectivestartdate < tpos.effectiveenddate
--   and pos.effectiveenddate >= tpos.effectiveenddate
-- left outer join cs_payee pay on
--   pos.payeeseq = pay.payeeseq
--   and pay.removedate = '2200-01-01'
--   and pay.effectivestartdate < pos.effectiveenddate
--   and pay.effectiveenddate >= pos.effectiveenddate
-- where jc.process_flag = 8
-- );



Insert into KYN_TQ2COM_PRESTAGE_QUOTA (select * from (
select
per.startdate as effectivestartdate,
per.enddate as effectiveenddate,
case jc.targettypeid
when 'GP' then 'Q_Profit'
else 'Q_'||jc.targettypeid
end as quotaname,
sum(jc.quotavalue) as value,
jc.unittype as unittypeforvalue,
pt.name as periodtypename,
null as businessunitmap,
jc.position as positionname,
qs.stagequota_batchname as batchname
from KYN_TQ2COM_TQ_QUOTA jc
join KYN_TQ2COM_SYNC qs on jc.run_key = qs.run_key
join cs_period per on qs.semiannual_periodseq = per.periodseq and per.removedate = '2200-01-01'
join cs_periodtype pt on per.periodtypeseq = pt.periodtypeseq and pt.removedate = '2200-01-01'
group by per.startdate, per.enddate, jc.targettypeid, jc.unittype, pt.name, jc.position, qs.stagequota_batchname
) x
where exists (select 1 from cs_quota q where q.removedate ='2200-01-01' and x.quotaname = q.name)
);


delete from KYN_TQ2COM_TQ_QUOTA where positionseq is null or targettypeid is null;


-- create table KYN_TQ2COM_COM_QUOTA as (
-- select
-- per.startdate as effectivestartdate, per.enddate as effectiveenddate,
-- quo.name quotaname, fv.value, ut.name as unittypeforvalue, pt.name periodtypename, 
-- null as businessunitmap, pos.name positionname
-- from 
-- cs_quota quo
-- join cs_calendar cal on quo.calendarseq = cal.calendarseq and cal.removedate = to_date('22000101','yyyymmdd') 
-- --left outer join cs_businessunit bu on bitand(quo.businessunitmap,bu.mask) > 0
-- join cs_unittype ut on quo.unittypeseq = ut.unittypeseq and ut.removedate = to_date('22000101','yyyymmdd')
-- join cs_quota_variables qv on
--   quo.quotaseq = qv.quotaseq
--   and qv.effectivestartdate < quo.effectiveenddate
--   and qv.effectiveenddate > quo.effectivestartdate
--   and qv.removedate = to_date('22000101','yyyymmdd')
--   and quo.removedate = to_date('22000101','yyyymmdd')
--   and qv.modelseq = 0
-- join cs_variableassignment vas on
--   qv.variableseq = vas.variableseq
--   and qv.effectivestartdate < vas.effectiveenddate
--   and qv.effectiveenddate > vas.effectivestartdate
--   and vas.removedate = to_date('22000101','yyyymmdd')
--   and vas.modelseq = 0
-- join cs_fixedvalue fv on
--   fv.ruleelementseq = vas.assignmentseq
--   and fv.effectivestartdate < vas.effectiveenddate
--   and fv.effectiveenddate > vas.effectivestartdate 
--   and fv.removedate = to_date('22000101','yyyymmdd')
--   and fv.modelseq = 0
-- join cs_periodtype pt on 
--   fv.periodtypeseq = pt.periodtypeseq
--   and pt.removedate = to_date('22000101','yyyymmdd')
-- join cs_position pos on
--   pos.ruleelementownerseq = vas.ruleelementownerseq 
--   and pos.effectiveenddate   >= fv.effectiveenddate 
--   and pos.effectivestartdate <  fv.effectiveenddate
--   and pos.removedate = to_date('22000101','yyyymmdd')
--   and pos.effectivestartdate = vas.effectivestartdate
-- join cs_period per on
--   fv.periodtypeseq = per.periodtypeseq
--   and per.startdate < fv.effectiveenddate
--   and per.enddate > fv.effectivestartdate
--   and per.removedate = to_date('22000101','yyyymmdd')
--   and per.calendarseq = cal.calendarseq
-- join cs_processingunit pu on
--   pos.processingunitseq = pu.processingunitseq  
-- where quo.modelseq = 0
-- );


Insert into KYN_TQ2COM_COM_QUOTA
 (
select
per.startdate as effectivestartdate, per.enddate as effectiveenddate,
quo.name quotaname, fv.value, ut.name as unittypeforvalue, pt.name periodtypename, 
null as businessunitmap, pos.name positionname
from 
cs_quota quo
join cs_calendar cal on quo.calendarseq = cal.calendarseq and cal.removedate = to_date('22000101','yyyymmdd') 
join cs_unittype ut on quo.unittypeseq = ut.unittypeseq and ut.removedate = to_date('22000101','yyyymmdd')
join cs_quota_variables qv on
  quo.quotaseq = qv.quotaseq
  and qv.effectivestartdate < quo.effectiveenddate
  and qv.effectiveenddate > quo.effectivestartdate
  and qv.removedate = to_date('22000101','yyyymmdd')
  and quo.removedate = to_date('22000101','yyyymmdd')
  and qv.modelseq = 0
join cs_variableassignment vas on
  qv.variableseq = vas.variableseq
  and qv.effectivestartdate < vas.effectiveenddate
  and qv.effectiveenddate > vas.effectivestartdate
  and vas.removedate = to_date('22000101','yyyymmdd')
  and vas.modelseq = 0
join cs_fixedvalue fv on
  fv.ruleelementseq = vas.assignmentseq
  and fv.effectivestartdate < vas.effectiveenddate
  and fv.effectiveenddate > vas.effectivestartdate 
  and fv.removedate = to_date('22000101','yyyymmdd')
  and fv.modelseq = 0
join cs_periodtype pt on 
  fv.periodtypeseq = pt.periodtypeseq
  and pt.removedate = to_date('22000101','yyyymmdd')
join cs_position pos on
  pos.ruleelementownerseq = vas.ruleelementownerseq 
  and pos.effectiveenddate   >= fv.effectiveenddate 
  and pos.effectivestartdate <  fv.effectiveenddate
  and pos.removedate = to_date('22000101','yyyymmdd')
  and pos.effectivestartdate = vas.effectivestartdate
join cs_period per on
  fv.periodtypeseq = per.periodtypeseq
  and per.startdate < fv.effectiveenddate
  and per.enddate > fv.effectivestartdate
  and per.removedate = to_date('22000101','yyyymmdd')
  and per.calendarseq = cal.calendarseq
join cs_processingunit pu on
  pos.processingunitseq = pu.processingunitseq  
where quo.modelseq = 0
);

