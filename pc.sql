select * from ext.kyn_tq2com_sync;
select * from ext.kyn_tq2com_filter;
select * from ext.kyn_tq2com_tq_quota;
select * from ext.kyn_tq2com_prestage_quota where run_key=1171;
select * from ext.kyn_tq2com_prestage_quota where run_key=1171;
select * from ext.kyn_tq2com_product where run_key=1171;

create or replace function EXT.KYN_FN_TQ2COM_QUOTA(i_positionSeq bigint default null)
returns v_ret varchar(32000) as
begin
  declare v_eot date := '2200-01-01';
  declare v_mdlt varchar(255) := 'LT_Bonus_Lookup';

declare cursor c_quota for
select row_number() over (order by it.semiannual_name, pq.position_name) as rn,
it.semiannual_name, name, pq.position_name, pq.effectivestartdate,pq.effectiveenddate,pq.quotaname, pq.value, pq.unittypeforvalue
from ext.kyn_tq2com_ipl_trace it,
ext.kyn_tq2com_prestage_quota pq
where it.run_key=pq.run_key
and pq.positionseq=:i_positionseq
-- and it.status='status_ApprovalsDone'
and it.acceptdate is not null
order by it.semiannual_name, pq.position_name;

 for x as c_quota
  do
    if :x.rn = 1 then      
      v_ret := '<p><b>'||'Quota Name'||' '||'Position Name'||'</b></p>'
            || '<table class="ruleElementTable table table-condensed">'
            || '<thead><tr><th>'||:x.quotaname||'</th><th>'||:x.position_name||'</th><th>Value</th></tr></thead>';
    end if;
    v_ret := :v_ret || '<tr><td>'||:x.quotaname||'</td><td>'||:x.dim1_value||'</td><td>'||:x.cell_value||'</td></tr>';  
  end for;
  v_ret := ifnull(:v_ret || '</table>', 'Quota  not found.');
end;


select * from ext.kyn_tq2com_sync;



create or replace function EXT.KYN_FN_TQ2COM_QUOTA(i_positionSeq bigint default null, i_periodSeq bigint default  null) 
returns v_ret varchar(32000) as
begin
  declare v_eot date := '2200-01-01';
  declare v_mdlt varchar(255) := 'LT_Bonus_Lookup';
  declare cursor c_mdlt for
  select mdlt.name as mdlt_name, re.description as mdlt_desc,
  dim0.name as dim0_name, ind0.minstring as dim0_value,
  dim1.name as dim1_name, to_char(cast(ind1.minvalue as integer)) as dim1_value,
  to_char(cast(cell.value as decimal(25,2))) as cell_value,
  row_number() over (order by ind0.displayorder, ind1.displayorder) as rn
  from cs_relationalmdlt mdlt
  join cs_ruleelement re on mdlt.ruleelementseq = re.ruleelementseq and re.removedate = :v_eot and re.effectivestartdate = mdlt.effectivestartdate
  join cs_mdltdimension dim0 on mdlt.ruleelementseq = dim0.ruleelementseq and dim0.removedate = :v_eot and dim0.dimensionslot = 0
  join cs_mdltindex ind0 on mdlt.ruleelementseq = ind0.ruleelementseq and ind0.removedate = :v_eot and ind0.dimensionseq = dim0.dimensionseq
  join cs_mdltdimension dim1 on mdlt.ruleelementseq = dim1.ruleelementseq and dim1.removedate = :v_eot and dim1.dimensionslot = 1
  join cs_mdltindex ind1 on mdlt.ruleelementseq = ind1.ruleelementseq and ind1.removedate = :v_eot and ind1.dimensionseq = dim1.dimensionseq
  left outer join cs_mdltcell cell on cell.mdltseq = mdlt.ruleelementseq and cell.removedate = :v_eot and cell.dim0index = ind0.ordinal and cell.dim1index = ind1.ordinal
  where mdlt.removedate = :v_eot 
  and mdlt.name = :v_mdlt
  order by ind0.displayorder, ind1.displayorder;  
  for x as c_mdlt
  do
    if :x.rn = 1 then      
      v_ret := '<p><b>'||:x.mdlt_name||' '||:x.mdlt_desc||'</b></p>'
            || '<table class="ruleElementTable table table-condensed">'
            || '<thead><tr><th>'||:x.dim0_name||'</th><th>'||:x.dim1_name||'</th><th>Value</th></tr></thead>';
    end if;
    v_ret := :v_ret || '<tr><td>'||:x.dim0_value||'</td><td>'||:x.dim1_value||'</td><td>'||:x.cell_value||'</td></tr>';  
  end for;
  v_ret := ifnull(:v_ret || '</table>', 'MDLT "'||:v_mdlt||'" not found.');
end;





insert into CS_PluginQuery (tenantId, name, query) values ((select tenantid from cs_tenant), 'Fetch_Quota', 
'select EXT.KYN_FN_TQ2COM_QUOTA(positionSeq) from (select $positionSeq as positionSeq from dummy)');


call ext.kyn_lib_tq2com:run();
