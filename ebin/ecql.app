%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% cassandra.app - OTP-compliant Application Configuration File
%%==============================================================================

  {
    application ,cassandra ,[
       {description ,"Cassandra"}
      ,{vsn ,"2014-04-17"}
      ,{modules ,[
         cassandra
        ,cassandra_cache
        ,cassandra_connection
        ,cassandra_perf
        ,cassandra_sender
        ,cassandra_stream
        ,cassandra_sup
        ,camnesia
      ]}
      ,{registered ,[
         cassandra
        ,cassandra_cache
        ,cassandra_sup
      ]}
      ,{applications ,[
         kernel
        ,sasl
        ,stdlib
      ]}
      ,{mod ,{cassandra ,[]}}
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
