
select * from ext.kyn_tq2com_sync;
select * from ext.kyn_tq2com_tq_quota;

update ext.kyn_tq2com_tq_quota set quotavalue=0 where position='PL2000_GENT_TQ'
and targettypeid='Signings' and territory_name='Worldwide' and run_key=144;

select * from ext.kyn_tq2com_prestage_quota;
select * from ext.kyn_tq2com_account;
select * from ext.kyn_tq2com_product;



call ext.kyn_lib_tq2com:get_territoryprogram_data('FY24H1_AM_Seller_WW_DI');
call ext.kyn_lib_tq2com:get_tq_from_program();
call ext.kyn_lib_tq2com:load_quota_to_prestage();
call ext.kyn_lib_tq2com:get_account_and_product_from_tq();
call  ext.kyn_lib_tq2com:generate_ipl_trace_info();
call ext.kyn_lib_tq2com:accept_ipl_quota(current_timestamp,
'Q_Signings_Minimum', 'PL2000_GENT_TQ', 
'HY1 2024');
call ext.kyn_lib_tq2com:accept_ipl_account(current_timestamp,
'169_169_DE-ALLIANZ', '5009345_GermanSchmidt_01', 
'HY1 2024','SellerAMRole0032'); --
call ext.kyn_lib_tq2com:accept_ipl_product(current_timestamp,
'CLOUD-AIS_TQ', '5009345_GermanSchmidt_01', 
'HY1 2024','SellerAMRole0032'); -- -- will need to update process_flag in kyn_tq2com_product table
call ext.kyn_lib_tq2com:load_stagequota (); -- will need to update batchname in kyn_tq2com_prestage_quota table,
                                            --also set process_flag = 3 for KYN_TQ2COM_IPL_TRACE table
call ext.kyn_lib_tq2com:trigger_quota_import();

