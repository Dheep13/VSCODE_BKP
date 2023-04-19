DO BEGIN

DECLARE vflag INTEGER;
DECLARE i INTEGER;
DECLARE start_time TIMESTAMP := CURRENT_UTCTIMESTAMP;
DECLARE end_time TIMESTAMP :=  ADD_SECONDS(start_time,10);


select count(distinct batchname) into vflag from cs_stagequota where batchname like 'TQ2COM_%' 
and stageprocessflag=0 and stageprocessdate is NULL;

-- SELECT lock_flag INTO vflag
-- FROM ext.tq2com_process_lock
-- WHERE process_name = 'Quota Import';

WHILE vflag <> 0 DO

 WHILE CURRENT_UTCTIMESTAMP <= end_time DO
          i=i+1;
  END WHILE;

select count(distinct batchname) into vflag from cs_stagequota where batchname like 'TQ2COM_%' 
and stageprocessflag=0 and stageprocessdate is NULL;

END WHILE;

--Run the library

END;