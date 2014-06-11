%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql.app - OTP-compliant Application Configuration File
%%==============================================================================

  {
    application ,ecql ,[
       {description ,"ecql"}
      ,{vsn ,"2014-06-11"}
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
        ,{keyspace, "ecql"}
        ,{replication_strategy, "SimpleStrategy"}
        ,{replication_factor, 2}
        %,{replication_strategy, "NetworkTopologyStrategy"}
        %,{replication_factor, [{"dc1", 3}, {"dc2", 2}]}
        ,{connections, 4}
        ,{streams_per_connection, 25}
      ]}
    ]
  }
.

%%==============================================================================
%% END OF FILE
