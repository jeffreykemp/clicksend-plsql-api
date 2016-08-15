begin
  clicksend_pkg.init
    (p_clicksend_username   => site_parameter.get_value('SMS_USERNAME')
    ,p_clicksend_secret_key => site_parameter.get_value('SMS_SECRET_KEY')
    ,p_api_url              => 'http://api.jk64.com/clicksend/v3/'
    );
end;
/

begin
  clicksend_pkg.init(p_voice_preamble => ',,,,,');
end;
/

exec clicksend_pkg.create_queue;

exec clicksend_pkg.create_job;

exec clicksend_pkg.create_purge_job;

begin
  clicksend_pkg.send_sms
    (p_sender  => 'tester'
    ,p_mobile  => '+61408288568' -- +61411111111 is a free test number, no msg will be sent or charged
    ,p_message => 'testing ' || to_char(systimestamp,'DD/MM/YYYY HH24:MI:SS.FF')
    );
  clicksend_pkg.push_queue;
  commit;
end;
/

begin
  clicksend_pkg.send_mms
    (p_sender         => 'tester'
    ,p_mobile         => '+61408288568' -- +61411111111 is a free test number, no msg will be sent or charged
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
    (p_phone_no     => '+61892741627' -- +61411111111 is a free test number, no msg will be sent or charged
    ,p_message      => 'Well, hello there. This message was sent on ' || to_char(systimestamp,'fmDay DD Month YYYY "at" HH:MI am, SS "seconds"')
                    || '. Lots of hugs and kisses from Jeff'
    ,p_voice_lang   => 'en-au'
    ,p_voice_gender => 'male'
    ,p_schedule_dt  => sysdate
    );
  clicksend_pkg.push_queue;
  commit;
end;
/

select clicksend_pkg.get_account_details from dual;

select clicksend_pkg.get_credit_balance from dual;

exec clicksend_pkg.drop_job;

exec clicksend_pkg.drop_purge_job;

exec clicksend_pkg.drop_queue;