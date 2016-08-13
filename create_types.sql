prompt create_types.sql
-- types used by clicksend 0.1

drop type t_clicksend_msg;

create type t_clicksend_msg is object
  ( message_type  varchar2(20) -- sms, mms, voice
  , requested_ts  timestamp
  , schedule_dt   date
  , sender        varchar2(20)
  , mobile        varchar2(20)
  , subject       varchar2(4000) -- for mms
  , message       varchar2(4000)
  , media_file    varchar2(4000) -- for mms
  , voice_lang    varchar2(100) -- for voice: e.g. en-us, en-au, en-gb
  , voice_gender  varchar2(6) -- for voice: female or male
  , country       varchar2(10 char)
  , reply_email   varchar2(255 char)
  , custom_string varchar2(4000)
  );
/