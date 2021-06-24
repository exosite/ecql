%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql.erl - Supervisor
%%==============================================================================
-module(ecql_sup).
-behaviour(supervisor).

-export([
   start_link/0
  ,init/1
]).

%%~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

%%------------------------------------------------------------------------------
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%------------------------------------------------------------------------------
init(_) ->
   MaxRestart = 600
  ,MaxTime = 600
  ,{
     ok
    ,{
       {one_for_one, MaxRestart, MaxTime}
      ,[
        worker(ecql_etsman)
       ,worker(ecql_cache)
       ,worker(ecql_erlcass_throttle)
       ,worker(ecql_native)
       ,worker(ecql_log)
       ,worker(ecql_replicator2)
       ,worker(ecql_monitor)
      ]
     }
   }
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Private API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
worker(Module) ->
  {
     Module
    ,{Module, start_link, []}
    ,permanent
    ,600
    ,worker
    ,[Module]
  }
.

%%==============================================================================
%% END OF FILE
