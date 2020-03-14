create or replace package body clicksend_pkg as
/* Clicksend API v0.2
  https://github.com/jeffreykemp/clicksend-plsql-api
  by Jeffrey Kemp
  Instrumented using Logger https://github.com/OraOpenSource/Logger
*/

scope_prefix constant varchar2(31) := lower($$plsql_unit) || '.';

queue_name             constant varchar2(30) := sys_context('userenv','current_schema')||'.clicksend_queue';
queue_table            constant varchar2(30) := sys_context('userenv','current_schema')||'.clicksend_queue_tab';
job_name               constant varchar2(30) := 'clicksend_process_queue';
purge_job_name         constant varchar2(30) := 'clicksend_purge_logs';
payload_type           constant varchar2(30) := sys_context('userenv','current_schema')||'.t_clicksend_msg';
max_dequeue_count      constant integer := 1000; -- max messages processed by push_queue in one go

-- defaults to use if init() not used to set these settings
default_country            constant varchar2(10)  := 'AU';
default_api_url            constant varchar2(200) := 'https://rest.clicksend.com/v3/';
default_voice_lang         constant varchar2(10)  := 'en-au'; -- aussie
default_voice_gender       constant varchar2(6)   := 'female';
default_voice_preamble     constant varchar2(500) := '.....'; -- add a pause at the start
default_log_retention_days constant number := 30;
default_queue_expiration   constant integer := 24 * 60 * 60; -- failed messages expire from the queue after 24 hours

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
setting_prod_instance_name     constant varchar2(100) := 'prod_instance_name';
setting_non_prod_recipient     constant varchar2(100) := 'non_prod_recipient';

type t_key_val_arr is table of varchar2(4000) index by varchar2(100);

g_setting t_key_val_arr;

e_no_queue_data       exception;
pragma exception_init (e_no_queue_data, -25228);

--------------------------------------------------------------------------------
--------------------------------- PRIVATE METHODS ------------------------------
--------------------------------------------------------------------------------

procedure assert (cond in boolean, err in varchar2) is
begin
  if not cond then
    raise_application_error(-20000, $$plsql_unit || ' assertion failed: ' || err);
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

-- retrieve all the settings for a normal session
procedure load_settings is
  scope logger_logs.scope%type := scope_prefix || 'load_settings';
  params logger.tab_param;
begin
  logger.log('START', scope, null, params);
  
  -- set defaults first
  g_setting(setting_api_url)              := default_api_url;
  g_setting(setting_wallet_path)          := '';
  g_setting(setting_wallet_password)      := '';
  g_setting(setting_log_retention_days)   := default_log_retention_days;
  g_setting(setting_default_sender)       := '';
  g_setting(setting_default_country)      := default_country;
  g_setting(setting_default_voice_lang)   := default_voice_lang;
  g_setting(setting_default_voice_gender) := default_voice_gender;
  g_setting(setting_voice_preamble)       := default_voice_preamble;
  g_setting(setting_queue_expiration)     := default_queue_expiration;
  g_setting(setting_prod_instance_name)   := '';
  g_setting(setting_non_prod_recipient)   := '';

  for r in (
    select s.setting_name
          ,s.setting_value
    from   clicksend_settings s
    ) loop
    
    g_setting(r.setting_name) := r.setting_value;
    
  end loop;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end load_settings;

procedure reset is
  scope logger_logs.scope%type := scope_prefix || 'setting';
  params logger.tab_param;
begin
  logger.log('START', scope, null, params);

  g_setting.delete;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end reset;

-- get a setting
-- if p_default is set, a null/not found will return the default value
-- if p_default is null, a not found will raise an exception
function setting (p_name in varchar2) return varchar2 is
  scope logger_logs.scope%type := scope_prefix || 'setting';
  params logger.tab_param;
  p_value clicksend_settings.setting_value%type;
begin
  logger.append_param(params,'p_name',p_name);
  logger.log('START', scope, null, params);

  assert(p_name is not null, 'p_name cannot be null');
  
  -- prime the settings array for this session
  if g_setting.count = 0 then
    load_settings;
  end if;
  
  p_value := g_setting(p_name);

  logger.log('END', scope, null, params);
  return p_value;
exception
  when no_data_found then
    logger.log_error('No Data Found', scope, null, params);
    raise_application_error(-20000, 'clicksend setting not set "' || p_name || '" - please setup using ' || $$plsql_unit || '.init()');
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end setting;

function log_retention_days return number is
begin
  return to_number(setting(setting_log_retention_days));
end log_retention_days;

procedure prod_check
  (p_is_prod            out boolean
  ,p_non_prod_recipient out varchar2
  ) is
  scope  logger_logs.scope%type := scope_prefix || 'prod_check';
  params logger.tab_param;
  prod_instance_name mailgun_settings.setting_value%type;
begin
  logger.log('START', scope, null, params);
  
  prod_instance_name := setting(setting_prod_instance_name);
  
  if prod_instance_name is not null then  
    p_is_prod := (prod_instance_name = sys_context('userenv','db_name'));
  else
    p_is_prod := true; -- if setting not set, we treat this as a prod env
  end if;
  
  if not p_is_prod then
    p_non_prod_recipient := setting(setting_non_prod_recipient);
  end if;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end prod_check;

procedure log_headers (resp in out nocopy sys.utl_http.resp) is
  scope logger_logs.scope%type := scope_prefix || 'log_headers';
  params logger.tab_param;
  name  varchar2(256);
  value varchar2(1024);
begin
  logger.log('START', scope, null, params);

  for i in 1..sys.utl_http.get_header_count(resp) loop
    sys.utl_http.get_header(resp, i, name, value);
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
  
  wallet_path := setting(setting_wallet_path);
  wallet_password := setting(setting_wallet_password);

  if wallet_path is not null or wallet_password is not null then
    sys.utl_http.set_wallet(wallet_path, wallet_password);
  end if;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end set_wallet;

function get_response (resp in out nocopy sys.utl_http.resp) return clob is
  scope logger_logs.scope%type := scope_prefix || 'get_response';
  params logger.tab_param;
  buf varchar2(32767);
  ret clob := empty_clob;
begin
  logger.log('START', scope, null, params);
  
  sys.dbms_lob.createtemporary(ret, true);

  begin
    loop
      sys.utl_http.read_text(resp, buf, 32767);
      sys.dbms_lob.writeappend(ret, length(buf), buf);
    end loop;
  exception
    when sys.utl_http.end_of_body then
      null;
  end;
  sys.utl_http.end_response(resp);

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
  ,p_params in varchar2 := null
  ,p_data   in varchar2 := null
  ,p_user   in varchar2 := null
  ,p_pwd    in varchar2 := null
  ,p_accept in varchar2 := null
  ) return clob is
  scope logger_logs.scope%type := scope_prefix || 'get_json';
  params logger.tab_param;
  url   varchar2(4000) := p_url;
  req   sys.utl_http.req;
  resp  sys.utl_http.resp;
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
  
  if p_params is not null then
    url := url || '?' || p_params;
  end if;

  req := sys.utl_http.begin_request(url => url, method => p_method);

  if p_user is not null or p_pwd is not null then
    logger.log('sys.utl_http.set_authentication', scope, null, params);
    sys.utl_http.set_authentication(req, p_user, p_pwd);
  end if;

  if p_data is not null then
    logger.log('sys.utl_http set headers Content-Type/Length', scope, null, params);
    sys.utl_http.set_header (req,'Content-Type','application/json');
    sys.utl_http.set_header (req,'Content-Length',length(p_data));
    logger.log('sys.utl_http.write_text', scope, null, params);
    sys.utl_http.write_text (req,p_data);
  end if;
  
  if p_accept is not null then
    sys.utl_http.set_header (req,'Accept',p_accept);
  end if;

  resp := sys.utl_http.get_response(req);
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

function epoch_to_dt (p_epoch in number) return date as
begin
  return date'1970-01-01' + (p_epoch / 24 / 60 / 60)
    + (systimestamp-sys_extract_utc(systimestamp));
end epoch_to_dt;

procedure url_param (buf in out varchar2, attr in varchar2, val in varchar2) is
  scope  logger_logs.scope%type := scope_prefix || 'url_param(1)';
  params logger.tab_param;
begin
  logger.append_param(params,'attr',attr);
  logger.append_param(params,'val',val);
  logger.log('START', scope, null, params);

  if val is not null then
    if buf is not null then
      buf := buf || '&';
    end if;
    buf := buf || attr || '=' || apex_util.url_encode(val);
  end if;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end url_param;

procedure url_param (buf in out varchar2, attr in varchar2, dt in date) is
  scope  logger_logs.scope%type := scope_prefix || 'url_param(2)';
  params logger.tab_param;
begin
  logger.append_param(params,'attr',attr);
  logger.append_param(params,'dt',dt);
  logger.log('START', scope, null, params);

  if dt is not null then
    if buf is not null then
      buf := buf || '&';
    end if;
    buf := buf || attr || '=' || get_epoch(dt);
  end if;

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end url_param;

procedure send_msg (p_payload in out nocopy t_clicksend_msg) as
  scope logger_logs.scope%type := scope_prefix || 'send_msg';
  params logger.tab_param;
  is_prod            boolean;
  non_prod_recipient varchar2(255);
  recipient          varchar2(255);
  payload            varchar2(32767);
  resp_text          varchar2(32767);
  
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
        -- log the parse problem but don't stop the logging
        logger.log_warning(SQLERRM, scope, resp_text, params);
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

  prod_check
    (p_is_prod            => is_prod
    ,p_non_prod_recipient => non_prod_recipient
    );

  if not is_prod and non_prod_recipient is not null then
  
    -- replace recipient with the non-prod recipient 
    recipient := non_prod_recipient;
    
  else
  
    recipient := p_payload.recipient;
  
  end if;
  
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
        apex_json.write('to', recipient);
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
  
  if is_prod or non_prod_recipient is not null then

    resp_text := get_json
      (p_url    => setting(setting_api_url) || p_payload.message_type || '/send'
      ,p_method => 'POST'
      ,p_data   => payload
      ,p_user   => setting(setting_clicksend_username)
      ,p_pwd    => setting(setting_clicksend_secret_key)
      );

  else
  
    logger.log_warning('message suppressed', scope, null, params);
  
    resp_text := 'message suppressed: ' || sys_context('userenv','db_name');
  
  end if;
  
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

-- comma-delimited list of attributes, plus values if required
function json_members_csv
  (p_path   in varchar2
  ,p0       in varchar2
  ,p_values in boolean
  ) return varchar2 is
  scope  logger_logs.scope%type := scope_prefix || 'json_members_csv';
  params logger.tab_param;
  arr wwv_flow_t_varchar2;
  buf varchar2(32767);
begin
  logger.append_param(params,'p_path',p_path);
  logger.append_param(params,'p0',p0);
  logger.append_param(params,'p_values',p_values);
  logger.log('START', scope, null, params);

  arr := apex_json.get_members(p_path, p0);
  if arr.count > 0 then
    for i in 1..arr.count loop
      if buf is not null then
        buf := buf || ',';
      end if;
      buf := buf || arr(i);
      if p_values then
        buf := buf || '=' || apex_json.get_varchar2(p_path || '.' || arr(i), p0);
      end if;
    end loop;
  end if;

  logger.log('END', scope, null, params);
  return buf;
exception
  when value_error /*not an array or object*/ then
    logger.log('END value_error', scope, null, params);
    return null;
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end json_members_csv;

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
  ,p_prod_instance_name   in varchar2 := default_no_change
  ,p_non_prod_recipient   in varchar2 := default_no_change
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
  logger.append_param(params,'p_prod_instance_name',p_prod_instance_name);
  logger.append_param(params,'p_non_prod_recipient',p_non_prod_recipient); 
  logger.log('START', scope, null, params);
  
  if nvl(p_clicksend_username,'*') != default_no_change then
    set_setting(setting_clicksend_username, p_clicksend_username);
  end if;

  if nvl(p_clicksend_secret_key,'*') != default_no_change then
    set_setting(setting_clicksend_secret_key, p_clicksend_secret_key);
  end if;

  if nvl(p_api_url,'*') != default_no_change then
    -- make sure the url ends with a /
    set_setting(setting_api_url, p_api_url
      || case when substr(p_api_url,-1,1) != '/' then '/' end);
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

  if nvl(p_prod_instance_name,'*') != default_no_change then
    set_setting(setting_prod_instance_name, p_prod_instance_name);
  end if;

  if nvl(p_non_prod_recipient,'*') != default_no_change then
    set_setting(setting_non_prod_recipient, p_non_prod_recipient);
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
  enq_opts        sys.dbms_aq.enqueue_options_t;
  enq_msg_props   sys.dbms_aq.message_properties_t;
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
  
  reset;
  
  assert(p_mobile is not null, 'p_mobile cannot be null');
  
  if substr(p_mobile, 1, 1) = '+' then
    assert(length(p_mobile) = 12, 'mobile starting with + must be 12 characters exactly (' || p_mobile || ')');
    assert(replace(translate(substr(p_mobile,2),'0123456789','-'),'-','') is null, 'mobile starting with + must have 11 digits (' || p_mobile || ')');
  else
    assert(length(p_mobile) = 10, 'mobile must have 10 digits exactly (' || p_mobile || ') (unless it starts with a +)');
    assert(replace(translate(p_mobile,'0123456789','-'),'-','') is null, 'mobile must be 10 digits (' || p_mobile || ') (unless it starts with a +)');
  end if;
  
  country := nvl(p_country, setting(setting_default_country));
  
  if country = 'AU' then
    assert(substr(p_mobile, 1, 2) in ('04', '05') or substr(p_mobile, 1, 4) in ('+614', '+615'), 'AU mobile must start with 04 or 05 (or +614 or +615)');
  end if;

  assert(p_message is not null, 'p_message cannot be null');
  assert(length(p_message) <= 960, 'maximum message length is 960 (' || length(p_message) || ')');
  
  sender := nvl(p_sender, setting(setting_default_sender));
  assert(sender is not null, 'sender cannot be null');
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

  enq_msg_props.expiration := setting(setting_queue_expiration);
  enq_msg_props.priority   := p_priority;

  sys.dbms_aq.enqueue
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
  enq_opts        sys.dbms_aq.enqueue_options_t;
  enq_msg_props   sys.dbms_aq.message_properties_t;
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
  
  reset;
  
  assert(p_mobile is not null, 'p_mobile cannot be null');

  assert(p_subject is not null, 'p_subject cannot be null');
  assert(length(p_subject) <= 20, 'maximum subject length is 20 (' || length(p_subject) || ')');

  assert(p_media_file_url is not null, 'p_media_file_url cannot be null');
  
  if substr(p_mobile, 1, 1) = '+' then
    assert(length(p_mobile) = 12, 'mobile starting with + must be 12 characters exactly (' || p_mobile || ')');
    assert(replace(translate(substr(p_mobile,2),'0123456789','-'),'-','') is null, 'mobile starting with + must have 11 digits (' || p_mobile || ')');
  else
    assert(length(p_mobile) = 10, 'mobile must have 10 digits exactly (' || p_mobile || ') (unless it starts with a +)');
    assert(replace(translate(p_mobile,'0123456789','-'),'-','') is null, 'mobile must be 10 digits (' || p_mobile || ') (unless it starts with a +)');
  end if;
  
  country := nvl(p_country, setting(setting_default_country));
  
  if country = 'AU' then
    assert(substr(p_mobile, 1, 2) in ('04', '05') or substr(p_mobile, 1, 4) in ('+614', '+615'), 'AU mobile must start with 04 or 05 (or +614 or +615)');
  end if;

  assert(p_message is not null, 'p_message cannot be null');
  assert(length(p_message) <= 1500, 'maximum message length is 1500 (' || length(p_message) || ')');
  
  sender := nvl(p_sender, setting(setting_default_sender));
  assert(sender is not null, 'sender cannot be null');
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

  enq_msg_props.expiration := setting(setting_queue_expiration);
  enq_msg_props.priority   := p_priority;

  sys.dbms_aq.enqueue
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
  enq_opts        sys.dbms_aq.enqueue_options_t;
  enq_msg_props   sys.dbms_aq.message_properties_t;
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

  reset;
  
  assert(p_phone_no is not null, 'p_phone_no cannot be null');
  
  if substr(p_phone_no, 1, 1) = '+' then
    assert(length(p_phone_no) = 12, 'phone_no starting with + must be 12 characters exactly (' || p_phone_no || ')');
    assert(replace(translate(substr(p_phone_no,2),'0123456789','-'),'-','') is null, 'phone_no starting with + must have 11 digits (' || p_phone_no || ')');
  else
    assert(length(p_phone_no) = 10, 'phone_no must have 10 digits exactly (' || p_phone_no || ') (unless it starts with a +)');
    assert(replace(translate(p_phone_no,'0123456789','-'),'-','') is null, 'phone_no must be 10 digits (' || p_phone_no || ') (unless it starts with a +)');
  end if;
  
  country := nvl(p_country, setting(setting_default_country));
  
  assert(p_message is not null, 'p_message cannot be null');
  message := substr(setting(setting_voice_preamble) || p_message, 1, 4000);
  assert(length(message) <= 1200, 'maximum message length is 1200 (' || length(message) || ') including preamble');

  voice_lang := nvl(p_voice_lang, setting(setting_default_voice_lang));
  assert(voice_lang is not null, 'voice_lang cannot be null');

  voice_gender := nvl(p_voice_gender, setting(setting_default_voice_gender));
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

  enq_msg_props.expiration := setting(setting_queue_expiration);
  enq_msg_props.priority   := p_priority;

  sys.dbms_aq.enqueue
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

  reset;

  v_json := get_json
    (p_url    => setting(setting_api_url) || 'account'
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

function get_languages return t_clicksend_lang_arr pipelined is
  scope logger_logs.scope%type := scope_prefix || 'get_languages';
  params logger.tab_param;
  v_json varchar2(32767);
  data_count number;
  gender apex_json.t_value;
  gender1 varchar2(10);
  gender2 varchar2(10);
begin
  logger.log('START', scope, null, params);

  v_json := get_json
    (p_url    => setting(setting_api_url) || 'voice/lang'
    ,p_method => 'GET'
    ,p_user   => setting(setting_clicksend_username)
    ,p_pwd    => setting(setting_clicksend_secret_key)
    ,p_accept => 'application/json'
    );
  
  apex_json.parse(v_json);
  
  data_count := apex_json.get_count('data');
  
  if data_count > 0 then
    for i in 1..data_count loop
      logger.log(i||' '||json_members_csv('data[%d]', i, p_values => true), scope, null, params);

      gender1 := null;
      gender2 := null;
      gender := apex_json.get_value('data[%d].gender', i);

      -- perversely, the gender node might be a simple value (e.g. "gender":"female")
      -- or it might be an array (e.g. "gender":["female","male"])
      if gender.kind = apex_json.c_varchar2 then
        gender1 := gender.varchar2_value;
      elsif gender.kind = apex_json.c_array then
        gender1 := apex_json.get_varchar2('data[%d].gender[1]', i);
        gender2 := apex_json.get_varchar2('data[%d].gender[2]', i);
      end if;

      pipe row (t_clicksend_lang
        (lang_code    => substr(apex_json.get_varchar2('data[%d].code', i), 1, 10)
        ,country_desc => substr(apex_json.get_varchar2('data[%d].country', i), 1, 100)
        ,female       => case when voice_female in (gender1,gender2) then 'Y' end
        ,male         => case when voice_male in (gender1,gender2) then 'Y' end
        ));

    end loop;
  end if;

  logger.log('END', scope);
  return;
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end get_languages;

function get_countries return t_clicksend_country_arr pipelined is
  scope logger_logs.scope%type := scope_prefix || 'get_countries';
  params logger.tab_param;
  v_json varchar2(32767);
  data_count number;
begin
  logger.log('START', scope, null, params);

  v_json := get_json
    (p_url    => setting(setting_api_url) || 'countries'
    ,p_method => 'GET'
    ,p_accept => 'application/json'
    );
  
  apex_json.parse(v_json);
  
  data_count := apex_json.get_count('data');
  
  if data_count > 0 then
    for i in 1..data_count loop
      logger.log(i||' '||json_members_csv('data[%d]', i, p_values => true), scope, null, params);

      pipe row (t_clicksend_country
        (country_code => substr(apex_json.get_varchar2('data[%d].code', i), 1, 10)
        ,country_name => substr(apex_json.get_varchar2('data[%d].value', i), 1, 100)
        ));

    end loop;
  end if;

  logger.log('END', scope);
  return;
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end get_countries;

function get_sms_history
  (p_from  in date := null -- default is 7 days prior to p_until
  ,p_until in date := null -- default is sysdate
  ) return t_clicksend_sms_history_arr pipelined is
  scope logger_logs.scope%type := scope_prefix || 'get_sms_history';
  params logger.tab_param;
  prm varchar2(4000);
  url varchar2(4000);
  v_json varchar2(32767);
  data_count number;
  page_count number := 1;
begin
  logger.append_param(params,'p_from',p_from);
  logger.append_param(params,'p_until',p_until);
  logger.log('START', scope, null, params);
  
  url_param(prm, 'date_from', nvl(p_from, nvl(p_until, sysdate) - 7));
  url_param(prm, 'date_to', nvl(p_until, sysdate));

  v_json := get_json
    (p_url    => setting(setting_api_url) || 'sms/history'
    ,p_method => 'GET'
    ,p_params => prm
    ,p_user   => setting(setting_clicksend_username)
    ,p_pwd    => setting(setting_clicksend_secret_key)
    ,p_accept => 'application/json'
    );
  
  loop
    
    apex_json.parse(v_json);
    
    data_count := apex_json.get_count('data.data');
    logger.log('total=' || apex_json.get_number('data.total'), scope, null, params);
    logger.log('current_page=' || apex_json.get_number('data.current_page'), scope, null, params);
    
    if data_count > 0 then
      for i in 1..data_count loop
        logger.log(i||' '||json_members_csv('data.data[%d]', i, p_values => true), scope, null, params);
  
        pipe row (t_clicksend_sms_history
          (event_dt      => epoch_to_dt(apex_json.get_varchar2('data.data[%d].date', i))
          ,mobile        => substr(apex_json.get_varchar2('data.data[%d].to', i), 1, 20)
          ,message       => substr(apex_json.get_varchar2('data.data[%d].body', i), 1, 4000)
          ,status        => substr(apex_json.get_varchar2('data.data[%d].status', i), 1, 100)
          ,sender        => substr(apex_json.get_varchar2('data.data[%d].from', i), 1, 100)
          ,schedule_dt   => epoch_to_dt(apex_json.get_varchar2('data.data[%d].schedule', i))
          ,status_code   => substr(apex_json.get_varchar2('data.data[%d].status_code', i), 1, 100)
          ,status_text   => substr(apex_json.get_varchar2('data.data[%d].status_text', i), 1, 4000)
          ,error_code    => substr(apex_json.get_varchar2('data.data[%d].error_code', i), 1, 100)
          ,error_text    => substr(apex_json.get_varchar2('data.data[%d].error_text', i), 1, 4000)
          ,message_id    => substr(apex_json.get_varchar2('data.data[%d].message_id', i), 1, 4000)
          ,message_parts => to_number(apex_json.get_varchar2('data.data[%d].message_parts', i))
          ,message_price => to_number(apex_json.get_varchar2('data.data[%d].message_price', i))
          ,reply_email   => substr(apex_json.get_varchar2('data.data[%d].from_email', i), 1, 255)
          ,custom_string => substr(apex_json.get_varchar2('data.data[%d].custom_string', i), 1, 4000)
          ,subaccount_id => apex_json.get_number('data.data[%d].subaccount_id', i)
          ,country       => substr(apex_json.get_varchar2('data.data[%d].country', i), 1, 10)
          ,carrier       => substr(apex_json.get_varchar2('data.data[%d].carrier', i), 1, 100)
          ));
  
      end loop;
    end if;
    
    url := apex_json.get_varchar2('data.next_page_url');
    logger.log('url='||url, scope, null, params);
    
    exit when url is null;
    
    v_json := get_json
      (p_url    => setting(setting_api_url) || 'sms/history' || url
      ,p_method => 'GET'
      ,p_user   => setting(setting_clicksend_username)
      ,p_pwd    => setting(setting_clicksend_secret_key)
      ,p_accept => 'application/json'
      );
    
    page_count := page_count + 1;
    
    exit when page_count > 10;
    
  end loop;

  logger.log('END', scope);
  return;
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end get_sms_history;

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

  sys.dbms_aqadm.create_queue_table
    (queue_table        => queue_table
    ,queue_payload_type => payload_type
    ,sort_list          => 'priority,enq_time'
    );

  sys.dbms_aqadm.create_queue
    (queue_name  => queue_name
    ,queue_table => queue_table
    ,max_retries => p_max_retries
    ,retry_delay => p_retry_delay
    );

  sys.dbms_aqadm.start_queue (queue_name);

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

  sys.dbms_aqadm.stop_queue (queue_name);
  
  sys.dbms_aqadm.drop_queue (queue_name);
  
  sys.dbms_aqadm.drop_queue_table (queue_table);  

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end drop_queue;

procedure purge_queue (p_msg_state IN VARCHAR2 := default_purge_msg_state) is
  scope logger_logs.scope%type := scope_prefix || 'purge_queue';
  params logger.tab_param;
  r_opt sys.dbms_aqadm.aq$_purge_options_t;
begin
  logger.append_param(params,'p_msg_state',p_msg_state);
  logger.log('START', scope, null, params);

  sys.dbms_aqadm.purge_queue_table
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
  r_dequeue_options    sys.dbms_aq.dequeue_options_t;
  r_message_properties sys.dbms_aq.message_properties_t;
  msgid                raw(16);
  payload              t_clicksend_msg;
  dequeue_count        integer := 0;
  job                  binary_integer;
begin
  logger.append_param(params,'p_asynchronous',p_asynchronous);
  logger.log('START', scope, null, params);

  if p_asynchronous then
  
    -- use dbms_job so that it is only run if/when this session commits
  
    sys.dbms_job.submit
      (job  => job
      ,what => $$plsql_unit || '.push_queue;'
      );
      
    logger.log('submitted job=' || job, scope, null, params);
      
  else

    reset;
    
    -- commit any messages requested in the current session
    logger.log('commit', scope, null, params);
    commit;
    
    r_dequeue_options.wait := sys.dbms_aq.no_wait;
  
    -- loop through all messages in the queue until there is none
    -- exit this loop when the e_no_queue_data exception is raised.
    loop    
  
      sys.dbms_aq.dequeue
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

  sys.dbms_scheduler.create_job
    (job_name        => job_name
    ,job_type        => 'stored_procedure'
    ,job_action      => $$plsql_unit||'.push_queue'
    ,start_date      => systimestamp
    ,repeat_interval => p_repeat_interval
    );

  sys.dbms_scheduler.set_attribute(job_name,'restartable',true);

  sys.dbms_scheduler.enable(job_name);

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
    sys.dbms_scheduler.stop_job (job_name);
  exception
    when others then
      if sqlcode != -27366 /*job already stopped*/ then
        raise;
      end if;
  end;
  
  sys.dbms_scheduler.drop_job (job_name);

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

  reset;

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

  sys.dbms_scheduler.create_job
    (job_name        => purge_job_name
    ,job_type        => 'stored_procedure'
    ,job_action      => $$plsql_unit||'.purge_logs'
    ,start_date      => systimestamp
    ,repeat_interval => p_repeat_interval
    );

  sys.dbms_scheduler.set_attribute(job_name,'restartable',true);

  sys.dbms_scheduler.enable(purge_job_name);

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
    sys.dbms_scheduler.stop_job (purge_job_name);
  exception
    when others then
      if sqlcode != -27366 /*job already stopped*/ then
        raise;
      end if;
  end;
  
  sys.dbms_scheduler.drop_job (purge_job_name);

  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    raise;
end drop_purge_job;

procedure send_test_sms
  (p_mobile               in varchar2
  ,p_message              in varchar2 := null
  ,p_sender               in varchar2 := null
  ,p_clicksend_username   in varchar2 := default_no_change
  ,p_clicksend_secret_key in varchar2 := default_no_change
  ,p_api_url              in varchar2 := default_no_change
  ,p_wallet_path          in varchar2 := default_no_change
  ,p_wallet_password      in varchar2 := default_no_change
  ) is
  scope logger_logs.scope%type := scope_prefix || 'send_test_sms';
  params logger.tab_param;
  payload t_clicksend_msg;
begin
  logger.append_param(params,'p_mobile',p_mobile);
  logger.append_param(params,'p_message',p_message);
  logger.append_param(params,'p_sender',p_sender);
  logger.append_param(params,'p_clicksend_username',p_clicksend_username);
  logger.append_param(params,'p_clicksend_secret_key',case when p_clicksend_secret_key is null then 'null' else 'not null' end);
  logger.append_param(params,'p_api_url',p_api_url);
  logger.append_param(params,'p_wallet_path',p_wallet_path);
  logger.append_param(params,'p_wallet_password',case when p_wallet_password is null then 'null' else 'not null' end);
  logger.log('START', scope, null, params);
  
  -- set up settings just for this call  
  load_settings;
  if p_clicksend_username != default_no_change then
    g_setting(setting_clicksend_username) := p_clicksend_username;
  end if;
  if p_clicksend_secret_key != default_no_change then
    g_setting(setting_clicksend_secret_key) := p_clicksend_secret_key;
  end if;
  if p_api_url != default_no_change then
    g_setting(setting_api_url) := p_api_url;
  end if;
  if p_wallet_path != default_no_change then
    g_setting(setting_wallet_path) := p_wallet_path;
  end if;
  if p_wallet_password != default_no_change then
    g_setting(setting_wallet_password) := p_wallet_password;
  end if;

  payload := t_clicksend_msg
    (message_type  => message_type_sms
    ,requested_ts  => systimestamp
    ,schedule_dt   => null
    ,sender        => nvl(p_sender, setting(setting_default_sender))
    ,recipient     => local_to_intnl_au(p_mobile, setting(setting_default_country))
    ,subject       => ''
    ,message       => nvl(p_message
                         ,'This test message was sent from '
                          || sys_context('userenv','db_name')
                          || ' at '
                          || to_char(systimestamp,'DD/MM/YYYY HH24:MI:SS.FF'))
    ,media_file    => ''
    ,voice_lang    => ''
    ,voice_gender  => ''
    ,country       => setting(setting_default_country)
    ,reply_email   => ''
    ,custom_string => ''
    );

  send_msg(p_payload => payload);
    
  -- reset everything back to normal  
  reset;
  
  logger.log('END', scope, null, params);
exception
  when others then
    logger.log_error('Unhandled Exception', scope, null, params);
    reset;
    raise;
end send_test_sms;

end clicksend_pkg;
/

show errors