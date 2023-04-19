select
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
  and t.removedate = TO_DATE('01/01/2200','mm/dd/yyyy')
  and t.effectivestartdate < jc.territoryprogram_eed
  and t.effectiveenddate > jc.territoryprogram_esd
join csq_territoryquota tq on 
  tq.territoryseq = t.territoryseq 
  and tq.removedate= TO_DATE('01/01/2200','mm/dd/yyyy')
  and tq.effectivestartdate < t.effectiveenddate
  and tq.effectiveenddate > t.effectivestartdate 
join csq_targettype tt on tq.targettypeseq = tt.datatypeseq and tt.removedate= TO_DATE('01/01/2200','mm/dd/yyyy')
join cs_unittype ut_tq on tq.unittypeforquotavalue = ut_tq.unittypeseq and ut_tq.removedate = TO_DATE('01/01/2200','mm/dd/yyyy')
join csq_territoryposition tpos on 
  tpos.territoryseq = t.territoryseq 
  and tpos.removedate = TO_DATE('01/01/2200','mm/dd/yyyy')
  and tpos.effectivestartdate < t.effectiveenddate
  and tpos.effectiveenddate > t.effectivestartdate 
join cs_position pos on 
  tpos.positionseq = pos.ruleelementownerseq 
  and pos.removedate = TO_DATE('01/01/2200','mm/dd/yyyy')
  and pos.effectivestartdate < tpos.effectiveenddate
  and pos.effectiveenddate >= tpos.effectiveenddate
join cs_payee pay on
  pos.payeeseq = pay.payeeseq
  and pay.removedate = TO_DATE('01/01/2200','mm/dd/yyyy')
  and pay.effectivestartdate < pos.effectiveenddate
  and pay.effectiveenddate >= pos.effectiveenddate
join ext.kyn_tq2com_filter fil on 
  pos.name = fil.filter_value
  and jc.run_key=fil.run_key
	
where jc.process_flag = 0
and jc.run_key=1095
