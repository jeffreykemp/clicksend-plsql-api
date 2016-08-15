prompt create_tables.sql
-- tables for clicksend v0.1

drop table clicksend_msg_log;

-- table to record logs of sent/attempted sms's
create table clicksend_msg_log
  ( message_type        varchar2(20 char) -- sms, mms, voice
  , requested_ts        timestamp
  , schedule_dt         date
  , sent_ts             timestamp
  , sender              varchar2(20 char)
  , recipient           varchar2(20 char)
  , subject             varchar2(4000) -- for mms
  , message             varchar2(4000)
  , media_file          varchar2(4000) -- for mms
  , voice_lang          varchar2(100 char) -- for voice: e.g. en-us, en-au, en-gb
  , voice_gender        varchar2(6 char) -- for voice: female or male
  , country             varchar2(10 char)
  , reply_email         varchar2(255 char)
  , custom_string       varchar2(4000)
  , clicksend_messageid varchar2(4000)
	, clicksend_result    varchar2(100 char)
	, clicksend_errortext varchar2(4000)
	, clicksend_cost      number
  , clicksend_response  varchar2(4000)
  );

-- table to store the clicksend parameters for this system
create table clicksend_settings
  ( setting_name    varchar2(100) not null
  , setting_value   varchar2(4000)
  , constraint clicksend_settings_pk primary key (setting_name)
  );