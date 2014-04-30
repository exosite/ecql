%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql.app - OTP-compliant Application Configuration File
%%==============================================================================

  {
    application ,ecql ,[
       {description ,"ecql"}
      ,{vsn ,"2014-04-17"}
      ,{modules ,[
         ecql
        ,ecql_cache
        ,ecql_connection
        ,ecql_sender
        ,ecql_stream
        ,ecql_sup
        ,ecql_mnesia
      ]}
      ,{registered ,[
         ecql
        ,ecql_cache
        ,ecql_sup
      ]}
      ,{applications ,[
         kernel
        ,sasl
        ,stdlib
      ]}
      ,{mod ,{ecql ,[]}}
      ,{env, [
         {user, ""}
        ,{pass, ""}
        ,{host, {127,0,0,1}}
        ,{port, 9042}
        ,{connections, 4}
        ,{streams_per_connection, 25}
      ]}
    ]
  }
.

%%==============================================================================
%% END OF FILE
