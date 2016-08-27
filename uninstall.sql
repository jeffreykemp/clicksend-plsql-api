prompt uninstall.sql
-- v0.1

prompt drop job (process queue)
begin dbms_scheduler.stop_job ('clicksend_process_queue'); exception when others then if sqlcode not in (-27366,-27475) then raise; end if; end;
/
begin dbms_scheduler.drop_job('clicksend_process_queue'); exception when others then if sqlcode not in (-27366,-27475) then raise; end if; end;
/

prompt drop job (purge logs)
begin dbms_scheduler.stop_job ('clicksend_purge_logs'); exception when others then if sqlcode not in (-27366,-27475) then raise; end if; end;
/
begin dbms_scheduler.drop_job('clicksend_purge_logs'); exception when others then if sqlcode not in (-27366,-27475) then raise; end if; end;
/

prompt drop queue
begin dbms_aqadm.stop_queue (user||'.clicksend_queue'); exception when others then if sqlcode!=-24010 then raise; end if; end;
/
begin dbms_aqadm.drop_queue (user||'.clicksend_queue'); exception when others then if sqlcode!=-24010 then raise; end if; end;
/
begin dbms_aqadm.drop_queue_table (user||'.clicksend_queue_tab'); exception when others then if sqlcode not in (-24010,-24002) then raise; end if; end;
/

prompt drop tables
begin execute immediate 'drop table clicksend_msg_log'; exception when others then if sqlcode!=-942 then raise; end if; end;
/
begin execute immediate 'drop table clicksend_settings'; exception when others then if sqlcode!=-942 then raise; end if; end;
/

prompt drop types
begin execute immediate 'drop type t_clicksend_msg'; exception when others then if sqlcode!=-4043 then raise; end if; end;
/
begin execute immediate 'drop type t_clicksend_lang_arr'; exception when others then if sqlcode!=-4043 then raise; end if; end;
/
begin execute immediate 'drop type t_clicksend_lang'; exception when others then if sqlcode!=-4043 then raise; end if; end;
/
begin execute immediate 'drop type t_clicksend_country_arr'; exception when others then if sqlcode!=-4043 then raise; end if; end;
/
begin execute immediate 'drop type t_clicksend_country'; exception when others then if sqlcode!=-4043 then raise; end if; end;
/
begin execute immediate 'drop type t_clicksend_sms_history_arr'; exception when others then if sqlcode!=-4043 then raise; end if; end;
/
begin execute immediate 'drop type t_clicksend_sms_history'; exception when others then if sqlcode!=-4043 then raise; end if; end;
/

prompt drop package
begin execute immediate 'drop package clicksend_pkg'; exception when others then if sqlcode!=-4043 then raise; end if; end;
/

prompt finished.