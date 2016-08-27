prompt create_types.sql
-- types used by clicksend 0.2

create type t_clicksend_msg is object
  ( message_type  varchar2(20) -- sms, mms, voice
  , requested_ts  timestamp
  , schedule_dt   date
  , sender        varchar2(20)
  , recipient     varchar2(20)
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

create type t_clicksend_lang is object
  (lang_code    varchar2(10)
  ,country_desc varchar2(100)
  ,female       varchar2(1)
  ,male         varchar2(1)
  );
/

create type t_clicksend_lang_arr is table of t_clicksend_lang;

create type t_clicksend_country is object
  (country_code varchar2(10)
  ,country_name varchar2(100)
  );
/

create type t_clicksend_country_arr is table of t_clicksend_country;
