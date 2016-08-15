prompt install.sql
prompt clicksend v0.1
-- run this script in the schema in which you wish the objects to be installed.

@create_tables.sql
@create_types.sql
@clicksend_pkg.pks
@clicksend_pkg_with_logger.pkb

prompt create queue
begin clicksend_pkg.create_queue; end;
/

prompt create scheduler jobs
begin clicksend_pkg.create_job; end;
/

begin clicksend_pkg.create_purge_job; end;
/

prompt attempt to recompile any invalid objects
begin dbms_utility.compile_schema(user,false); end;
/

set feedback off heading off

prompt list clicksend objects
select object_type, object_name, status from user_objects where object_name like '%clicksend%' order by object_type, object_name;

prompt list clicksend queues
select name, queue_table from user_queues where name like '%clicksend%' order by name;

prompt list clicksend scheduler jobs
select job_name, 'enabled='||enabled status, job_action, repeat_interval from user_scheduler_jobs where job_name like '%clicksend%';

prompt finished.
set feedback on heading on