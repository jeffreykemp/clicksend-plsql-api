create or replace package clicksend_pkg1 as
/* Clicksend API v0.2
  https://github.com/jeffreykemp/clicksend-plsql-api
  by Jeffrey Kemp
*/

default_no_change             constant varchar2(4000) := '*NO-CHANGE*';
default_priority              constant number := 3;
default_repeat_interval       constant varchar2(200) := 'FREQ=MINUTELY;INTERVAL=5;';
default_purge_repeat_interval constant varchar2(200) := 'FREQ=WEEKLY;BYDAY=SUN;BYHOUR=0;';
default_purge_msg_state       constant varchar2(100) := 'EXPIRED';
default_max_retries           constant number := 10; --allow failures before giving up on a message
default_retry_delay           constant number := 60; --wait seconds before trying this message again

-- country (note: this list is not complete)
country_afghanistan           constant varchar2(2) := 'AF';
country_australia             constant varchar2(2) := 'AU';
country_finland               constant varchar2(2) := 'FI';
country_france                constant varchar2(2) := 'FR';
country_united_arab_emirates  constant varchar2(2) := 'AE';
country_united_kingdom        constant varchar2(2) := 'GB';
country_usa                   constant varchar2(2) := 'US';
country_zimbabwe              constant varchar2(2) := 'ZW';
-- refer http://docs.clicksend.apiary.io/#reference/countries/country-collection/get-all-countries

-- voice language (note: this list might be incomplete)
voice_american                constant varchar2(10) := 'en-us'; -- male or female
voice_australian              constant varchar2(10) := 'en-au'; -- male or female
voice_british                 constant varchar2(10) := 'en-gb'; -- male or female
voice_french                  constant varchar2(10) := 'fr-fr'; -- male or female
voice_canadian_french         constant varchar2(10) := 'fr-ca'; -- female only
voice_chinese                 constant varchar2(10) := 'zh-cn'; -- male or female
voice_dutch                   constant varchar2(10) := 'nl-nl'; -- male or female
voice_german                  constant varchar2(10) := 'de-de'; -- male or female
voice_indian_english          constant varchar2(10) := 'en-in'; -- female only
voice_icelandic               constant varchar2(10) := 'is-is'; -- male or female
voice_italian                 constant varchar2(10) := 'it-it'; -- male or female
voice_japanese                constant varchar2(10) := 'ja-jp'; -- male or female
voice_korean                  constant varchar2(10) := 'ko-kr'; -- male or female
voice_mexican                 constant varchar2(10) := 'es-mx'; -- female only
voice_polish                  constant varchar2(10) := 'pl-pl'; -- male or female
voice_portuguese              constant varchar2(10) := 'pt-br'; -- male or female
voice_romanian                constant varchar2(10) := 'ro-ro'; -- female only
voice_russian                 constant varchar2(10) := 'ru-ru'; -- female only
voice_spanish                 constant varchar2(10) := 'es-es'; -- male or female
voice_spanish_us              constant varchar2(10) := 'es-us'; -- male or female
voice_swedish                 constant varchar2(10) := 'sv-se'; -- female only
voice_turkish                 constant varchar2(10) := 'tr-tr'; -- female only
-- refer http://docs.clicksend.apiary.io/#reference/voice/voice-languages/voice-languages

-- voice gender
voice_female                  constant varchar2(10) := 'female';
voice_male                    constant varchar2(10) := 'male';

-- init: set up clicksend parameters
--   default is to not change the given parameter
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
  );

procedure send_sms
  (p_mobile        in varchar2
  ,p_message       in varchar2
  ,p_sender        in varchar2 := null
  ,p_schedule_dt   in date     := null -- default is ASAP
  ,p_country       in varchar2 := null
  ,p_reply_email   in varchar2 := null
  ,p_custom_string in varchar2 := null
  ,p_priority      in number   := default_priority -- lower numbers are processed first
  );

procedure send_mms
  (p_mobile         in varchar2
  ,p_subject        in varchar2
  ,p_message        in varchar2
  ,p_media_file_url in varchar2 -- must be a jpg or gif, up to 250kB, anything else must be converted first; some devices only accept up to 30kB
  ,p_sender         in varchar2 := null
  ,p_schedule_dt    in date     := null -- default is ASAP
  ,p_country        in varchar2 := null
  ,p_reply_email    in varchar2 := null
  ,p_custom_string  in varchar2 := null
  ,p_priority       in number   := default_priority -- lower numbers are processed first
  );

procedure send_voice
  (p_phone_no       in varchar2
  ,p_message        in varchar2
  ,p_voice_lang     in varchar2 := null
  ,p_voice_gender   in varchar2 := null
  ,p_schedule_dt    in date     := null -- default is ASAP
  ,p_country        in varchar2 := null
  ,p_custom_string  in varchar2 := null
  ,p_priority       in number   := default_priority -- lower numbers are processed first
  );

function get_account_details return varchar2;

function get_credit_balance return number;

function get_languages return t_clicksend_lang_arr pipelined;

function get_countries return t_clicksend_country_arr pipelined;

function get_sms_history
  (p_from  in date := null -- default is 7 days ago
  ,p_until in date := null -- default is sysdate
  ) return t_clicksend_sms_history_arr pipelined;

-- create the queue for asynchronous sms's
procedure create_queue
  (p_max_retries in number := default_max_retries
  ,p_retry_delay in number := default_retry_delay
  );

-- drop the queue
procedure drop_queue;

-- purge any expired (failed) emails stuck in the queue
procedure purge_queue (p_msg_state in varchar2 := default_purge_msg_state);

-- send emails in the queue
procedure push_queue
  (p_asynchronous in boolean := false);

-- create a job to periodically call push_queue
procedure create_job
  (p_repeat_interval in varchar2 := default_repeat_interval);

-- drop the push_queue job
procedure drop_job;

-- purge the logs older than the given number of days
procedure purge_logs (p_log_retention_days in number := null);

-- create a job to periodically call purge_logs
procedure create_purge_job
  (p_repeat_interval in varchar2 := default_purge_repeat_interval);

-- drop the purge_logs job
procedure drop_purge_job;

-- Send a test sms.
-- Can be used to test a setting without storing it.
-- Bypasses the queue.
procedure send_test_sms
  (p_mobile               in varchar2
  ,p_message              in varchar2 := null
  ,p_sender               in varchar2 := null
  ,p_clicksend_username   in varchar2 := default_no_change
  ,p_clicksend_secret_key in varchar2 := default_no_change
  ,p_api_url              in varchar2 := default_no_change
  ,p_wallet_path          in varchar2 := default_no_change
  ,p_wallet_password      in varchar2 := default_no_change
  );

end clicksend_pkg1;
/

show errors