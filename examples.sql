begin
  clicksend_pkg.init
    (p_clicksend_username   => '...your Clicksend username...'
    ,p_clicksend_secret_key => '...your Clicksend secret key...'
    );
end;
/

-- for Reverse Proxy method
begin
  clicksend_pkg.init
    (p_api_url => 'http://api.yourhost.com/clicksend/v3/'
    );
end;
/

-- setup for voice
begin
  clicksend_pkg.init
    (p_default_voice_lang   => 'en-au'
    ,p_default_voice_gender => 'female'
    );
end;
/

exec clicksend_pkg.create_queue;

exec clicksend_pkg.create_job;

exec clicksend_pkg.create_purge_job;

begin
  clicksend_pkg.send_sms
    (p_sender  => 'tester'
    ,p_mobile  => '+61411111111' -- +61411111111 is a free test number, no msg will be sent or charged
    ,p_message => 'testing ' || to_char(systimestamp,'DD/MM/YYYY HH24:MI:SS.FF')
    );
  clicksend_pkg.push_queue;
  commit;
end;
/

begin
  clicksend_pkg.send_mms
    (p_sender         => 'tester'
    ,p_mobile         => '+61411111111' -- +61411111111 is a free test number, no msg will be sent or charged
    ,p_subject        => 'testing mms'
    ,p_message        => 'testing ' || to_char(systimestamp,'DD/MM/YYYY HH24:MI:SS.FF')
    ,p_media_file_url => 'https://s3-ap-southeast-2.amazonaws.com/jk64/jk64logo.jpg'
    );
  clicksend_pkg.push_queue;
  commit;
end;
/

begin
  clicksend_pkg.send_voice
    (p_mobile       => '+61411111111' -- +61411111111 is a free test number, no msg will be sent or charged
    ,p_message      => 'Hello World, sent ' || to_char(systimestamp,'fmDay DD Month YYYY "at" HH:MI am, SS "seconds"')
    ,p_voice_lang   => 'en-au'
    ,p_voice_gender => 'female'
    );
  clicksend_pkg.push_queue;
  commit;
end;
/

select clicksend_pkg.get_account_details from dual;

select clicksend_pkg.get_credit_balance from dual;

exec clicksend_pkg.drop_purge_job;

exec clicksend_pkg.drop_job;

exec clicksend_pkg.drop_queue;