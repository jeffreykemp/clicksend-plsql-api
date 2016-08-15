create or replace package body clicksend_pkg as
-- clicksend package instrumented with Logger

scope_prefix constant varchar2(31) := lower($$plsql_unit) || '.';

default_log_retention_days constant number := 30;
default_queue_expiration   constant integer := 24 * 60 * 60; -- failed messages expire from the queue after 24 hours

queue_name             constant varchar2(30) := sys_context('userenv','current_schema')||'.clicksend_queue';
queue_table            constant varchar2(30) := sys_context('userenv','current_schema')||'.clicksend_queue_tab';
job_name               constant varchar2(30) := 'clicksend_process_queue';
purge_job_name         constant varchar2(30) := 'clicksend_purge_logs';
payload_type           constant varchar2(30) := sys_context('userenv','current_schema')||'.t_clicksend_msg';
max_dequeue_count      constant integer := 1000; -- max messages processed by push_queue in one go

-- defaults to use if init() not used to set these settings
default_country        constant varchar2(10)  := 'AU';
default_api_url        constant varchar2(200) := 'https://rest.clicksend.com/v3/';
default_voice_gender   constant varchar2(6)   := 'female';
default_voice_preamble constant varchar2(500) := '.....'; -- add a pause at the start

-- message types
message_type_sms       constant varchar2(20) := 'sms';
message_type_mms       constant varchar2(20) := 'mms';
message_type_voice     constant varchar2(20) := 'voice';

-- setting names
setting_clicksend_username     constant varchar2(100) := 'clicksend_username';
setting_clicksend_secret_key   constant varchar2(100) := 'clicksend_secret_key';
setting_api_url                constant varchar2(100) := 'api_url';
setting_wallet_path            constant varchar2(100) := 'wallet_path';
setting_wallet_password        constant varchar2(100) := 'wallet_password';
setting_log_retention_days     constant varchar2(100) := 'log_retention_days';
setting_default_sender         constant varchar2(100) := 'default_sender';
setting_default_country        constant varchar2(100) := 'default_country';
setting_default_voice_lang     constant varchar2(100) := 'default_voice_lang';
setting_default_voice_gender   constant varchar2(100) := 'default_voice_gender';
setting_voice_preamble         constant varchar2(100) := 'voice_preamble';
setting_queue_expiration       constant varchar2(100) := 'queue_expiration';

-- dummy "null" value
default_null constant varchar2(100) := '*NULL*';

e_no_queue_data       exception;
pragma exception_init (e_no_queue_data, -25228);

--------------------------------------------------------------------------------
--------------------------------- PRIVATE METHODS ------------------------------
--------------------------------------------------------------------------------

procedure assert (cond in boolean, err in varchar2) is
begin
  if not cond then
    raise_application_error(-20000, $$PLSQL_UNIT || ' assertion failed: ' || err);
  end if;
end assert;

-- set or update a setting
procedure set_setting
  (p_name  in varchar2
  ,p_value in varchar2
  ) is
  scope logger_logs.scope%type := scope_prefix || 'set_setting';
  params logger.tab_param;
begin
  logger.append_param(params,'p_name',p_name);
  logger.append_param(params,'p_value',case when p_value is null then 'null' else 'not null' end);
  logger.log('START', scope, null, params);
  
  assert(p_name is not null, 'p_name cannot be null');
  
  merge into clicksend_settings t
  using (select p_name  as setting_name
               ,p_value as setting_value
         from dual) s
    on (t.setting_name = s.setting_name)
  when matched then
    update set t.setting_value = s.setting_value
  when not matched then
    insert (setting_name, setting_value)
    values (s.setting_name, s.setting_value);
  
  logger.log('MERGE clicksend_settings: ' || SQL%ROWCOUNT, scope, null, params);
  
  logger.log('commit', scope, null, params);
  commit;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end set_setting;

-- get a setting
-- if p_default is set, a null/not found will return the default value
-- if p_default is null, a not found will raise an exception
function setting
  (p_name    in varchar2
  ,p_default in varchar2 := null
  ) return varchar2 result_cache is
  scope logger_logs.scope%type := scope_prefix || 'setting';
  params logger.tab_param;
  p_value clicksend_settings.setting_value%type;
begin
  logger.append_param(params,'p_name',p_name);
  logger.log('START', scope, null, params);

  assert(p_name is not null, 'p_name cannot be null');
  
  select s.setting_value
  into   p_value
  from   clicksend_settings s
  where  s.setting_name = setting.p_name;

  logger.log('END', scope, null, params);
  return nvl(p_value, p_default);
exception
  when no_data_found then
    if p_default is not null then
      return p_default;
    else
      logger.log_error('No Data Found', scope, null, params);
      raise_application_error(-20000, 'clicksend setting not set "' || p_name || '" - please setup using ' || $$plsql_unit || '.init()');
    end if;
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end setting;

function api_url return varchar2 is
begin
  return setting(setting_api_url, p_default => default_api_url);
end api_url;

function log_retention_days return number is
begin
  return to_number(setting(setting_log_retention_days, p_default => default_log_retention_days));
end log_retention_days;

procedure log_headers (resp in out nocopy utl_http.resp) is
  scope logger_logs.scope%type := scope_prefix || 'log_headers';
  params logger.tab_param;
  name  varchar2(256);
  value varchar2(1024);
begin
  logger.log('START', scope, null, params);

  for i in 1..utl_http.get_header_count(resp) loop
    utl_http.get_header(resp, i, name, value);
    logger.log(name || ': ' || value, scope, null, params);
  end loop;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end log_headers;

procedure set_wallet is
  scope logger_logs.scope%type := scope_prefix || 'set_wallet';
  params logger.tab_param;
  wallet_path     varchar2(4000);
  wallet_password varchar2(4000);
begin
  logger.log('START', scope, null, params);
  
  wallet_path := setting(setting_wallet_path, p_default => default_null);
  wallet_password := setting(setting_wallet_password, p_default => default_null);

  if wallet_path != default_null or wallet_password != default_null then
    utl_http.set_wallet(wallet_path, wallet_password);
  end if;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end set_wallet;

function get_response (resp in out nocopy utl_http.resp) return clob is
  scope logger_logs.scope%type := scope_prefix || 'get_response';
  params logger.tab_param;
  buf varchar2(32767);
  ret clob := empty_clob;
begin
  logger.log('START', scope, null, params);
  
  dbms_lob.createtemporary(ret, true);

  begin
    loop
      utl_http.read_text(resp, buf, 32767);
      dbms_lob.writeappend(ret, length(buf), buf);
    end loop;
  exception
    when utl_http.end_of_body then
      null;
  end;
  utl_http.end_response(resp);

  logger.log('END', scope, ret, params);
  return ret;
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end get_response;

function get_json
  (p_url    in varchar2
  ,p_method in varchar2
  ,p_data   in varchar2 := null
  ,p_user   in varchar2 := null
  ,p_pwd    in varchar2 := null
  ,p_accept in varchar2 := null
  ) return clob is
  scope logger_logs.scope%type := scope_prefix || 'get_json';
  params logger.tab_param;
  url   varchar2(4000) := p_url;
  req   utl_http.req;
  resp  utl_http.resp;
  ret   clob;
begin
  logger.append_param(params,'p_url',p_url);
  logger.append_param(params,'p_method',p_method);
  logger.append_param(params,'p_data',p_data);
  logger.append_param(params,'p_user',p_user);
  logger.append_param(params,'p_pwd',CASE WHEN p_pwd IS NOT NULL THEN '(not null)' ELSE 'NULL' END);
  logger.append_param(params,'p_accept',p_accept);
  logger.log('START', scope, null, params);

  assert(p_url is not null, 'get_json: p_url cannot be null');
    
  set_wallet;

  req := utl_http.begin_request(url => p_url, method => p_method);

  if p_user is not null or p_pwd is not null then
    logger.log('utl_http.set_authentication', scope, null, params);
    utl_http.set_authentication(req, p_user, p_pwd);
  end if;

  if p_data is not null then
    logger.log('utl_http set headers Content-Type/Length', scope, null, params);
    utl_http.set_header (req,'Content-Type','application/json');
    utl_http.set_header (req,'Content-Length',length(p_data));
    logger.log('utl_http.write_text', scope, null, params);
    utl_http.write_text (req,p_data);
  end if;
  
  if p_accept is not null then
    utl_http.set_header (req,'Accept',p_accept);
  end if;

  resp := utl_http.get_response(req);
  logger.log('HTTP response: ' || resp.status_code || ' ' || resp.reason_phrase, scope, null, params);

  log_headers(resp);

  if resp.status_code != '200' then
    raise_application_error(-20000, 'get_json call failed ' || resp.status_code || ' ' || resp.reason_phrase || ' [' || url || ']');
  end if;

  ret := get_response(resp);

  logger.log('END', scope, ret, params);
  return ret;
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end get_json;

function get_epoch (p_date in date) return number as
  date_utc date;
begin
  date_utc := sys_extract_utc(cast(p_date as timestamp));
  return trunc((date_utc - date'1970-01-01') * 24 * 60 * 60);
end get_epoch;

procedure send_msg (p_payload in out nocopy t_clicksend_msg) as
  scope logger_logs.scope%type := scope_prefix || 'send_msg';
  params logger.tab_param;
  payload varchar2(32767);
  resp_text varchar2(32767);
  
  procedure log_response is
    -- needs to commit the log entry independently of calling transaction
    pragma autonomous_transaction;
    log clicksend_msg_log%rowtype;
  begin
    logger.log('log_response', scope, null, params);

    log.sent_ts            := systimestamp;
    log.message_type       := p_payload.message_type;
    log.requested_ts       := p_payload.requested_ts;
    log.schedule_dt        := p_payload.schedule_dt;
    log.sender             := p_payload.sender;
    log.recipient          := p_payload.recipient;
    log.subject            := p_payload.subject;
    log.message            := p_payload.message;
    log.media_file         := p_payload.media_file;
    log.voice_lang         := p_payload.voice_lang;
    log.voice_gender       := p_payload.voice_gender;
    log.country            := p_payload.country;
    log.reply_email        := p_payload.reply_email;
    log.custom_string      := p_payload.custom_string;
    log.clicksend_response := substr(resp_text, 1, 4000);
    
    begin
      apex_json.parse(resp_text);

      log.clicksend_messageid := apex_json.get_varchar2('data.messages[1].message_id');
      log.clicksend_result    := apex_json.get_number('http_code');
      log.clicksend_errortext := apex_json.get_varchar2('response_code');
      log.clicksend_cost      := apex_json.get_number('data.total_price');
    exception
      when others then
        -- log the error but don't stop the logging
        logger.log_error(SQLERRM, scope, resp_text, params);
    end;

    insert into clicksend_msg_log values log;
    logger.log('inserted clicksend_msg_log: ' || sql%rowcount, scope, null, params);

    logger.log('commit', scope, null, params);
    commit;
    
  end log_response;

begin
  logger.append_param(params,'p_payload.message_type',p_payload.message_type);
  logger.append_param(params,'p_payload.requested_ts',p_payload.requested_ts);
  logger.append_param(params,'p_payload.schedule_dt',p_payload.schedule_dt);
  logger.append_param(params,'p_payload.sender',p_payload.sender);
  logger.append_param(params,'p_payload.recipient',p_payload.recipient);
  logger.append_param(params,'p_payload.subject',p_payload.subject);
  logger.append_param(params,'p_payload.message',p_payload.message);
  logger.append_param(params,'p_payload.media_file',p_payload.media_file);
  logger.append_param(params,'p_payload.voice_lang',p_payload.voice_lang);
  logger.append_param(params,'p_payload.voice_gender',p_payload.voice_gender);
  logger.append_param(params,'p_payload.country',p_payload.country);
  logger.append_param(params,'p_payload.reply_email',p_payload.reply_email);
  logger.append_param(params,'p_payload.custom_string',p_payload.custom_string);
  logger.log('START', scope, null, params);
  
  assert(p_payload.message_type in (message_type_sms, message_type_mms, message_type_voice)
        ,'message_type must be sms, mms or voice');
  
  begin
    apex_json.initialize_clob_output;
    apex_json.open_object;
      if p_payload.media_file is not null then
        apex_json.write('media_file', p_payload.media_file);
      end if;
      apex_json.open_array('messages');
        apex_json.open_object;
        apex_json.write('source', 'oracle');
        if p_payload.message_type in (message_type_sms, message_type_mms) then
          apex_json.write('from', p_payload.sender);
        end if;
        if p_payload.message_type = message_type_mms then
          apex_json.write('subject', p_payload.subject);
        end if;
        apex_json.write('body', p_payload.message);
        apex_json.write('to', p_payload.recipient);
        if p_payload.message_type = message_type_voice then
          apex_json.write('lang', p_payload.voice_lang);
          apex_json.write('voice', p_payload.voice_gender);
        end if;
        if p_payload.schedule_dt is not null then
          apex_json.write('schedule', get_epoch(p_payload.schedule_dt));
        end if;
        if p_payload.custom_string is not null then
          apex_json.write('custom_string', p_payload.custom_string);
        end if;
        if p_payload.country is not null then
          apex_json.write('country', p_payload.country);
        end if;
        if p_payload.reply_email is not null then
          apex_json.write('from_email', p_payload.reply_email);
        end if;
    apex_json.close_all;
    payload := apex_json.get_clob_output;    
    apex_json.free_output;     
  exception
    when others then
      apex_json.free_output;
      raise;
  end;

  resp_text := get_json
    (p_url    => api_url || p_payload.message_type || '/send'
    ,p_method => 'POST'
    ,p_data   => payload
    ,p_user   => setting(setting_clicksend_username)
    ,p_pwd    => setting(setting_clicksend_secret_key)
    );
  
  log_response;

  logger.log('END', scope);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end send_msg;

-- convert '0408123456' to '+61408123456'
function local_to_intnl_au
  (p_mobile  in varchar2
  ,p_country in varchar2
  ) return varchar2 is
  scope logger_logs.scope%type := scope_prefix || 'local_to_intnl_au';
  params logger.tab_param;
  ret varchar2(20) := substr(p_mobile, 1, 20);
begin
  logger.append_param(params,'p_mobile',p_mobile);  
  logger.log('START', scope, null, params);

  if substr(ret, 1, 1) != '+'
  and p_country = 'AU' then
    ret := '+61' || substr(ret, 2);
  end if;

  logger.log('END', scope);
  return ret;
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end local_to_intnl_au;

--------------------------------------------------------------------------------
--------------------------------- PUBLIC METHODS ------------------------------
--------------------------------------------------------------------------------

procedure init
  (p_clicksend_username   in varchar2 := default_no_change
  ,p_clicksend_secret_key in varchar2 := default_no_change
  ,p_api_url              in varchar2 := default_no_change
  ,p_wallet_path          in varchar2 := default_no_change
  ,p_wallet_password      in varchar2 := default_no_change
  ,p_default_sender       in varchar2 := default_no_change
  ,p_default_country      in varchar2 := default_no_change
  ,p_default_voice_lang   in varchar2 := default_no_change
  ,p_default_voice_gender in varchar2 := default_no_change
  ,p_voice_preamble       in varchar2 := default_no_change
  ,p_log_retention_days   in number := null
  ,p_queue_expiration     in number := null
  ) is
  scope logger_logs.scope%type := scope_prefix || 'init';
  params logger.tab_param;
begin
  logger.append_param(params,'p_clicksend_username',p_clicksend_username);
  logger.append_param(params,'p_clicksend_secret_key',case when p_clicksend_secret_key is null then 'null' else 'not null' end);
  logger.append_param(params,'p_api_url',p_api_url);
  logger.append_param(params,'p_wallet_path',p_wallet_path);
  logger.append_param(params,'p_wallet_password',case when p_wallet_password is null then 'null' else 'not null' end);
  logger.append_param(params,'p_log_retention_days',p_log_retention_days);
  logger.append_param(params,'p_default_sender',p_default_sender);
  logger.append_param(params,'p_default_country',p_default_country);
  logger.append_param(params,'p_default_voice_lang',p_default_voice_lang);
  logger.append_param(params,'p_default_voice_gender',p_default_voice_gender);
  logger.append_param(params,'p_voice_preamble',p_voice_preamble);
  logger.append_param(params,'p_queue_expiration',p_queue_expiration);  
  logger.log('START', scope, null, params);
  
  if nvl(p_clicksend_username,'*') != default_no_change then
    set_setting(setting_clicksend_username, p_clicksend_username);
  end if;

  if nvl(p_clicksend_secret_key,'*') != default_no_change then
    set_setting(setting_clicksend_secret_key, p_clicksend_secret_key);
  end if;

  if nvl(p_api_url,'*') != default_no_change then
    set_setting(setting_api_url, p_api_url);
  end if;

  if nvl(p_wallet_path,'*') != default_no_change then
    set_setting(setting_wallet_path, p_wallet_path);
  end if;

  if nvl(p_wallet_password,'*') != default_no_change then
    set_setting(setting_wallet_password, p_wallet_password);
  end if;

  if nvl(p_default_sender,'*') != default_no_change then
    set_setting(setting_default_sender, p_default_sender);
  end if;
  
  if nvl(p_default_country,'*') != default_no_change then
    set_setting(setting_default_country, p_default_country);
  end if;

  if nvl(p_default_voice_lang,'*') != default_no_change then
    set_setting(setting_default_voice_lang, p_default_voice_lang);
  end if;

  if nvl(p_default_voice_gender,'*') != default_no_change then
    set_setting(setting_default_voice_gender, p_default_voice_gender);
  end if;

  if nvl(p_voice_preamble,'*') != default_no_change then
    set_setting(setting_voice_preamble, p_voice_preamble);
  end if;

  if p_log_retention_days is not null then
    set_setting(setting_log_retention_days, p_log_retention_days);
  end if;

  if p_queue_expiration is not null then
    set_setting(setting_queue_expiration, p_queue_expiration);
  end if;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end init;

procedure send_sms
  (p_mobile        in varchar2
  ,p_message       in varchar2
  ,p_sender        in varchar2 := null
  ,p_schedule_dt   in date     := null -- default is ASAP
  ,p_country       in varchar2 := null
  ,p_reply_email   in varchar2 := null
  ,p_custom_string in varchar2 := null
  ,p_priority      in number   := default_priority -- lower numbers are processed first
  ) is
  scope logger_logs.scope%type := scope_prefix || 'send_sms';
  params logger.tab_param;
  enq_opts        dbms_aq.enqueue_options_t;
  enq_msg_props   dbms_aq.message_properties_t;
  payload         t_clicksend_msg;
  msgid           raw(16);
  sender          varchar2(100);
  country         varchar2(10);
begin
  logger.append_param(params,'p_mobile',p_mobile);
  logger.append_param(params,'p_message',p_message);
  logger.append_param(params,'p_sender',p_sender);
  logger.append_param(params,'p_schedule_dt',p_schedule_dt);
  logger.append_param(params,'p_country',p_country);
  logger.append_param(params,'p_reply_email',p_reply_email);
  logger.append_param(params,'p_custom_string',p_custom_string);
  logger.append_param(params,'p_priority',p_priority);
  logger.log('START', scope, null, params);
  
  assert(p_mobile is not null, 'p_mobile cannot be null');
  
  if substr(p_mobile, 1, 1) = '+' then
    assert(length(p_mobile) = 12, 'mobile starting with + must be 12 characters exactly (' || p_mobile || ')');
    assert(replace(translate(substr(p_mobile,2),'0123456789','-'),'-','') is null, 'mobile starting with + must have 11 digits (' || p_mobile || ')');
  else
    assert(length(p_mobile) = 10, 'mobile must have 10 digits exactly (' || p_mobile || ') (unless it starts with a +)');
    assert(replace(translate(p_mobile,'0123456789','-'),'-','') is null, 'mobile must be 10 digits (' || p_mobile || ') (unless it starts with a +)');
  end if;
  
  country := nvl(p_country, setting(setting_default_country, default_country));
  
  if country = 'AU' then
    assert(substr(p_mobile, 1, 2) in ('04', '05') or substr(p_mobile, 1, 4) in ('+614', '+615'), 'AU mobile must start with 04 or 05 (or +614 or +615)');
  end if;

  assert(p_message is not null, 'p_message cannot be null');
  assert(length(p_message) <= 960, 'maximum message length is 960 (' || length(p_message) || ')');
  
  sender := nvl(p_sender, setting(setting_default_sender, default_null));
  assert(sender != default_null, 'sender cannot be null');
  assert(length(sender) <= 11, 'sender cannot be >11 characters (' || sender || ')');
  
  assert(length(p_reply_email) <= 255, 'p_reply_email cannot be >255 characters');
  assert(length(p_custom_string) <= 4000, 'p_custom_string cannot be >4000 characters');

  payload := t_clicksend_msg
    (message_type  => message_type_sms
    ,requested_ts  => systimestamp
    ,schedule_dt   => p_schedule_dt
    ,sender        => sender
    ,recipient     => local_to_intnl_au(p_mobile, country)
    ,subject       => ''
    ,message       => p_message
    ,media_file    => ''
    ,voice_lang    => ''
    ,voice_gender  => ''
    ,country       => country
    ,reply_email   => p_reply_email
    ,custom_string => p_custom_string
    );

  enq_msg_props.expiration := setting(setting_queue_expiration, default_queue_expiration);
  enq_msg_props.priority   := p_priority;

  dbms_aq.enqueue
    (queue_name         => queue_name
    ,enqueue_options    => enq_opts
    ,message_properties => enq_msg_props
    ,payload            => payload
    ,msgid              => msgid
    );

  logger.log('msg queued ' || msgid, scope, null, params);

  logger.log('END', scope);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end send_sms;

procedure send_mms
  (p_mobile         in varchar2
  ,p_subject        in varchar2
  ,p_message        in varchar2
  ,p_media_file_url in varchar2
  ,p_sender         in varchar2 := null
  ,p_schedule_dt    in date     := null -- default is ASAP
  ,p_country        in varchar2 := null
  ,p_reply_email    in varchar2 := null
  ,p_custom_string  in varchar2 := null
  ,p_priority       in number   := default_priority -- lower numbers are processed first
  ) is
  scope logger_logs.scope%type := scope_prefix || 'send_mms';
  params logger.tab_param;
  enq_opts        dbms_aq.enqueue_options_t;
  enq_msg_props   dbms_aq.message_properties_t;
  payload         t_clicksend_msg;
  msgid           raw(16);
  sender          varchar2(100);
  country         varchar2(10);
begin
  logger.append_param(params,'p_mobile',p_mobile);
  logger.append_param(params,'p_subject',p_subject);
  logger.append_param(params,'p_message',p_message);
  logger.append_param(params,'p_media_file_url',p_media_file_url);
  logger.append_param(params,'p_sender',p_sender);
  logger.append_param(params,'p_schedule_dt',p_schedule_dt);
  logger.append_param(params,'p_country',p_country);
  logger.append_param(params,'p_reply_email',p_reply_email);
  logger.append_param(params,'p_custom_string',p_custom_string);
  logger.append_param(params,'p_priority',p_priority);
  logger.log('START', scope, null, params);
  
  assert(p_mobile is not null, 'p_mobile cannot be null');
  assert(p_subject is not null, 'p_subject cannot be null');
  assert(p_media_file_url is not null, 'p_media_file_url cannot be null');
  
  if substr(p_mobile, 1, 1) = '+' then
    assert(length(p_mobile) = 12, 'mobile starting with + must be 12 characters exactly (' || p_mobile || ')');
    assert(replace(translate(substr(p_mobile,2),'0123456789','-'),'-','') is null, 'mobile starting with + must have 11 digits (' || p_mobile || ')');
  else
    assert(length(p_mobile) = 10, 'mobile must have 10 digits exactly (' || p_mobile || ') (unless it starts with a +)');
    assert(replace(translate(p_mobile,'0123456789','-'),'-','') is null, 'mobile must be 10 digits (' || p_mobile || ') (unless it starts with a +)');
  end if;
  
  country := nvl(p_country, setting(setting_default_country, default_country));
  
  if country = 'AU' then
    assert(substr(p_mobile, 1, 2) in ('04', '05') or substr(p_mobile, 1, 4) in ('+614', '+615'), 'AU mobile must start with 04 or 05 (or +614 or +615)');
  end if;

  assert(p_message is not null, 'p_message cannot be null');
  assert(length(p_message) <= 1500, 'maximum message length is 1500 (' || length(p_message) || ')');
  
  sender := nvl(p_sender, setting(setting_default_sender, default_null));
  assert(sender != default_null, 'sender cannot be null');
  assert(length(sender) <= 11, 'sender cannot be >11 characters (' || sender || ')');
  
  assert(length(p_reply_email) <= 255, 'p_reply_email cannot be >255 characters');
  assert(length(p_custom_string) <= 4000, 'p_custom_string cannot be >4000 characters');

  payload := t_clicksend_msg
    (message_type  => message_type_mms
    ,requested_ts  => systimestamp
    ,schedule_dt   => p_schedule_dt
    ,sender        => sender
    ,recipient     => local_to_intnl_au(p_mobile, country)
    ,subject       => p_subject
    ,message       => p_message
    ,media_file    => p_media_file_url
    ,voice_lang    => ''
    ,voice_gender  => ''
    ,country       => country
    ,reply_email   => p_reply_email
    ,custom_string => p_custom_string
    );

  enq_msg_props.expiration := setting(setting_queue_expiration, default_queue_expiration);
  enq_msg_props.priority   := p_priority;

  dbms_aq.enqueue
    (queue_name         => queue_name
    ,enqueue_options    => enq_opts
    ,message_properties => enq_msg_props
    ,payload            => payload
    ,msgid              => msgid
    );

  logger.log('msg queued ' || msgid, scope, null, params);

  logger.log('END', scope);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end send_mms;

procedure send_voice
  (p_phone_no       in varchar2
  ,p_message        in varchar2
  ,p_voice_lang     in varchar2 := null
  ,p_voice_gender   in varchar2 := null
  ,p_schedule_dt    in date     := null -- default is ASAP
  ,p_country        in varchar2 := null
  ,p_custom_string  in varchar2 := null
  ,p_priority       in number   := default_priority -- lower numbers are processed first
  ) is
  scope logger_logs.scope%type := scope_prefix || 'send_voice';
  params logger.tab_param;
  enq_opts        dbms_aq.enqueue_options_t;
  enq_msg_props   dbms_aq.message_properties_t;
  payload         t_clicksend_msg;
  msgid           raw(16);
  message         varchar2(4000);
  voice_lang      varchar2(100);
  voice_gender    varchar2(6);
  country         varchar2(10);
begin
  logger.append_param(params,'p_phone_no',p_phone_no);
  logger.append_param(params,'p_message',p_message);
  logger.append_param(params,'p_voice_lang',p_voice_lang);
  logger.append_param(params,'p_voice_gender',p_voice_gender);
  logger.append_param(params,'p_schedule_dt',p_schedule_dt);
  logger.append_param(params,'p_country',p_country);
  logger.append_param(params,'p_custom_string',p_custom_string);
  logger.append_param(params,'p_priority',p_priority);
  logger.log('START', scope, null, params);
  
  assert(p_phone_no is not null, 'p_phone_no cannot be null');
  
  if substr(p_phone_no, 1, 1) = '+' then
    assert(length(p_phone_no) = 12, 'phone_no starting with + must be 12 characters exactly (' || p_phone_no || ')');
    assert(replace(translate(substr(p_phone_no,2),'0123456789','-'),'-','') is null, 'phone_no starting with + must have 11 digits (' || p_phone_no || ')');
  else
    assert(length(p_phone_no) = 10, 'phone_no must have 10 digits exactly (' || p_phone_no || ') (unless it starts with a +)');
    assert(replace(translate(p_phone_no,'0123456789','-'),'-','') is null, 'phone_no must be 10 digits (' || p_phone_no || ') (unless it starts with a +)');
  end if;
  
  country := nvl(p_country, setting(setting_default_country, default_country));
  
  assert(p_message is not null, 'p_message cannot be null');
  message := substr(setting(setting_voice_preamble, default_voice_preamble) || p_message, 1, 4000);
  assert(length(message) <= 1200, 'maximum message length is 1200 (' || length(message) || ') including preamble');

  voice_lang := nvl(p_voice_lang, setting(setting_default_voice_lang, default_null));
  assert(voice_lang != default_null, 'voice_lang cannot be null');

  voice_gender := nvl(p_voice_gender, setting(setting_default_voice_gender, default_voice_gender));
  assert(voice_gender in ('female','male'), 'voice_gender must be female or male');
  
  assert(length(p_custom_string) <= 4000, 'p_custom_string cannot be >4000 characters');

  payload := t_clicksend_msg
    (message_type  => message_type_voice
    ,requested_ts  => systimestamp
    ,schedule_dt   => p_schedule_dt
    ,sender        => ''
    ,recipient     => p_phone_no
    ,subject       => ''
    ,message       => message
    ,media_file    => ''
    ,voice_lang    => voice_lang
    ,voice_gender  => voice_gender
    ,country       => country
    ,reply_email   => ''
    ,custom_string => p_custom_string
    );

  enq_msg_props.expiration := setting(setting_queue_expiration, default_queue_expiration);
  enq_msg_props.priority   := p_priority;

  dbms_aq.enqueue
    (queue_name         => queue_name
    ,enqueue_options    => enq_opts
    ,message_properties => enq_msg_props
    ,payload            => payload
    ,msgid              => msgid
    );

  logger.log('msg queued ' || msgid, scope, null, params);

  logger.log('END', scope);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end send_voice;

function get_account_details return varchar2 is
pragma autonomous_transaction;
  scope logger_logs.scope%type := scope_prefix || 'get_account_details';
  params logger.tab_param;
  v_json  varchar2(32767);
begin
  logger.log('START', scope, null, params);

  v_json := get_json
    (p_url    => api_url || 'account'
    ,p_method => 'GET'
    ,p_user   => setting(setting_clicksend_username)
    ,p_pwd    => setting(setting_clicksend_secret_key)
    ,p_accept => 'application/json'
    );

  logger.log('END', scope);
  return v_json;
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end get_account_details;

function get_credit_balance return number is
  scope logger_logs.scope%type := scope_prefix || 'get_credit_balance';
  params logger.tab_param;
  v_json   varchar2(4000);
  v_bal    varchar2(4000);
begin
  logger.log('START', scope, null, params);

  v_json := get_account_details;

  apex_json.parse(v_json);

  v_bal := apex_json.get_varchar2('data.balance');

  logger.log('END', scope);
  return to_number(v_bal);
exception
  when value_error then
    logger.log_error('get_credit_balance: unable to convert balance "' || v_bal || '"', scope, null, params);
    raise;
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end get_credit_balance;

procedure create_queue
  (p_max_retries in number := default_max_retries
  ,p_retry_delay in number := default_retry_delay
  ) is
  scope logger_logs.scope%type := scope_prefix || 'create_queue';
  params logger.tab_param;
begin
  logger.append_param(params,'p_max_retries',p_max_retries);
  logger.append_param(params,'p_retry_delay',p_retry_delay);
  logger.log('START', scope, null, params);

  dbms_aqadm.create_queue_table
    (queue_table        => queue_table
    ,queue_payload_type => payload_type
    ,sort_list          => 'priority,enq_time'
    );

  dbms_aqadm.create_queue
    (queue_name  => queue_name
    ,queue_table => queue_table
    ,max_retries => p_max_retries
    ,retry_delay => p_retry_delay
    );

  dbms_aqadm.start_queue (queue_name);

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end create_queue;

procedure drop_queue is
  scope logger_logs.scope%type := scope_prefix || 'drop_queue';
  params logger.tab_param;
begin
  logger.log('START', scope, null, params);

  dbms_aqadm.stop_queue (queue_name);
  
  dbms_aqadm.drop_queue (queue_name);
  
  dbms_aqadm.drop_queue_table (queue_table);  

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end drop_queue;

procedure purge_queue (p_msg_state IN VARCHAR2 := default_purge_msg_state) is
  scope logger_logs.scope%type := scope_prefix || 'purge_queue';
  params logger.tab_param;
  r_opt dbms_aqadm.aq$_purge_options_t;
begin
  logger.append_param(params,'p_msg_state',p_msg_state);
  logger.log('START', scope, null, params);

  dbms_aqadm.purge_queue_table
    (queue_table     => queue_table
    ,purge_condition => case when p_msg_state is not null
                        then replace(q'[ qtview.msg_state = '#STATE#' ]'
                                    ,'#STATE#', p_msg_state)
                        end
    ,purge_options   => r_opt);

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end purge_queue;

procedure push_queue
  (p_asynchronous in boolean := false) as
  scope logger_logs.scope%type := scope_prefix || 'push_queue';
  params logger.tab_param;
  r_dequeue_options    dbms_aq.dequeue_options_t;
  r_message_properties dbms_aq.message_properties_t;
  msgid                raw(16);
  payload              t_clicksend_msg;
  dequeue_count        integer := 0;
  job                  binary_integer;
begin
  logger.append_param(params,'p_asynchronous',p_asynchronous);
  logger.log('START', scope, null, params);

  if p_asynchronous then
  
    -- use dbms_job so that it is only run if/when this session commits
  
    dbms_job.submit
      (job  => job
      ,what => $$PLSQL_UNIT || '.push_queue;'
      );
      
    logger.log('submitted job=' || job, scope, null, params);
      
  else
    
    -- commit any messages requested in the current session
    logger.log('commit', scope, null, params);
    commit;
    
    r_dequeue_options.wait := dbms_aq.no_wait;
  
    -- loop through all messages in the queue until there is none
    -- exit this loop when the e_no_queue_data exception is raised.
    loop    
  
      dbms_aq.dequeue
        (queue_name         => queue_name
        ,dequeue_options    => r_dequeue_options
        ,message_properties => r_message_properties
        ,payload            => payload
        ,msgid              => msgid
        );
      
      logger.log('payload priority: ' || r_message_properties.priority
        || ' enqeued: ' || to_char(r_message_properties.enqueue_time,'dd/mm/yyyy hh24:mi:ss')
        || ' attempts: ' || r_message_properties.attempts
        , scope, null, params);
  
      -- process the message
      send_msg (p_payload => payload);  
  
      logger.log('commit', scope, null, params);
      commit; -- the queue will treat the message as succeeded
      
      -- don't bite off everything in one go
      dequeue_count := dequeue_count + 1;
      exit when dequeue_count >= max_dequeue_count;
    end loop;

  end if;

  logger.log('END', scope, null, params);
exception
  when e_no_queue_data then
    logger.log('END push_queue finished count=' || dequeue_count, scope, null, params);
  when others then
    rollback; -- the queue will treat the message as failed
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end push_queue;

procedure create_job
  (p_repeat_interval in varchar2 := default_repeat_interval) is
  scope logger_logs.scope%type := scope_prefix || 'create_job';
  params logger.tab_param;
begin
  logger.append_param(params,'p_repeat_interval',p_repeat_interval);
  logger.log('START', scope, null, params);

  assert(p_repeat_interval is not null, 'create_job: p_repeat_interval cannot be null');

  dbms_scheduler.create_job
    (job_name        => job_name
    ,job_type        => 'stored_procedure'
    ,job_action      => $$PLSQL_UNIT||'.push_queue'
    ,start_date      => systimestamp
    ,repeat_interval => p_repeat_interval
    );

  dbms_scheduler.set_attribute(job_name,'restartable',true);

  dbms_scheduler.enable(job_name);

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end create_job;

procedure drop_job is
  scope logger_logs.scope%type := scope_prefix || 'drop_job';
  params logger.tab_param;
begin
  logger.log('START', scope, null, params);

  begin
    dbms_scheduler.stop_job (job_name);
  exception
    when others then
      if sqlcode != -27366 /*job already stopped*/ then
        raise;
      end if;
  end;
  
  dbms_scheduler.drop_job (job_name);

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end drop_job;

procedure purge_logs (p_log_retention_days in number := null) is
  scope logger_logs.scope%type := scope_prefix || 'purge_logs';
  params logger.tab_param;
  l_log_retention_days number;
begin
  logger.append_param(params,'p_log_retention_days',p_log_retention_days);
  logger.log('START', scope, null, params);

  l_log_retention_days := nvl(p_log_retention_days, log_retention_days);
  logger.append_param(params,'l_log_retention_days',l_log_retention_days);
  
  delete clicksend_msg_log
  where requested_ts < sysdate - l_log_retention_days;
  
  logger.log_info('DELETED clicksend_msg_log: ' || SQL%ROWCOUNT, scope, null, params);
  
  logger.log('commit', scope, null, params);
  commit;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end purge_logs;

procedure create_purge_job
  (p_repeat_interval in varchar2 := default_purge_repeat_interval) is
  scope logger_logs.scope%type := scope_prefix || 'create_purge_job';
  params logger.tab_param;
begin
  logger.append_param(params,'p_repeat_interval',p_repeat_interval);
  logger.log('START', scope, null, params);

  assert(p_repeat_interval is not null, 'create_purge_job: p_repeat_interval cannot be null');

  dbms_scheduler.create_job
    (job_name        => purge_job_name
    ,job_type        => 'stored_procedure'
    ,job_action      => $$PLSQL_UNIT||'.purge_logs'
    ,start_date      => systimestamp
    ,repeat_interval => p_repeat_interval
    );

  dbms_scheduler.set_attribute(job_name,'restartable',true);

  dbms_scheduler.enable(purge_job_name);

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end create_purge_job;

procedure drop_purge_job is
  scope logger_logs.scope%type := scope_prefix || 'drop_purge_job';
  params logger.tab_param;
begin
  logger.log('START', scope, null, params);

  begin
    dbms_scheduler.stop_job (purge_job_name);
  exception
    when others then
      if sqlcode != -27366 /*job already stopped*/ then
        raise;
      end if;
  end;
  
  dbms_scheduler.drop_job (purge_job_name);

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end drop_purge_job;

end clicksend_pkg;
/

show errors