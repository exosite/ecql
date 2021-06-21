%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_monitor.erl - Monitors the cache fill rate and starts/stops replication.
%%==============================================================================
-module(ecql_monitor).
-behaviour(gen_server).

%% OTP gen_server
-export([
   init/1
  ,start_link/0
  ,stop/0
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
  ,terminate/2
]).

%% Includes
-include("ecql.hrl").

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {}, [])
.

%%------------------------------------------------------------------------------
init(_) ->
   timer:send_interval(1000, ping)
  ,ok = net_kernel:monitor_nodes(true)
  ,{ok, #{want_replication => nil}}
.

%%------------------------------------------------------------------------------
stop() ->
  gen_server:call(?MODULE, stop)
.

%%------------------------------------------------------------------------------
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}
.

%%------------------------------------------------------------------------------
handle_cast({want_replication, nil, _Node}, State) ->
   {noreply, State}
;

%%------------------------------------------------------------------------------
handle_cast({want_replication, true, Node}, State) ->
   Nodes = ecql_cache:replication_nodes()
  ,case lists:member(Node, Nodes) of
    true -> ok;
    false -> ecql_cache:set_replication_nodes([Node | Nodes])
  end
  ,{noreply, State}
;

%%------------------------------------------------------------------------------
handle_cast({want_replication, false, Node}, State) ->
   Nodes = ecql_cache:replication_nodes()
  ,NewNodes = lists:delete(Node, Nodes)
  ,ecql_cache:set_replication_nodes(NewNodes)
  ,{noreply, State}
;

%%------------------------------------------------------------------------------
handle_cast(terminate, State) ->
  {stop, terminated, State}
.

%%------------------------------------------------------------------------------
handle_info(ping, State = #{want_replication := WR}) ->
  %% Checking local cache size and disabling want bit if >50%
   Capacity = ecql_cache:cache_size()
  ,Infos = [ets:info(Table) || Table <- ?CACHE_SLICES_LIST]
  ,Count = lists:foldl(fun(Info, C) ->
    C + proplists:get_value(size, Info)
  end, 0, Infos)
  ,NewWR = (Count * 2) < Capacity
  ,NewState = State#{want_replication => NewWR}
  %% Informing other nodes of a change
  ,NewWR =/= WR andalso
    gen_server:abcast(ecql_cache:nodes(), ecql_monitor, {want_replication, NewWR, node()})
  ,{noreply, NewState}
;

%%------------------------------------------------------------------------------
handle_info({nodeup, Node}, State = #{want_replication := WR}) ->
   gen_server:cast({Node, ecql_monitor}, {want_replication, WR, node()})
  ,{noreply, State}
;

%%------------------------------------------------------------------------------
handle_info({nodedown, Node}, State) ->
   Nodes = ecql_cache:replication_nodes()
  ,NewNodes = lists:delete(Node, Nodes)
  ,ecql_cache:set_replication_nodes(NewNodes)
  ,{noreply, State}
;

%%------------------------------------------------------------------------------
handle_info(Other, State) ->
   error_logger:error_msg("ecql_monitor: Received ~p", [Other])
  ,{noreply, State}
.

%%------------------------------------------------------------------------------
terminate(_Reason, State) ->
  {shutdown, State}
.

%%------------------------------------------------------------------------------
code_change(_, State, _) ->
  {ok, State}
.

%%==============================================================================
%% END OF FILE
