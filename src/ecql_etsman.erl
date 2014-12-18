%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_etsman.erl - Owns all ets tables to protectect them crashes.
%%==============================================================================
-module(ecql_etsman).
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

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {} ,[])
.

%%------------------------------------------------------------------------------
init(_) ->
   ecql_cache = ets:new(ecql_cache ,[named_table, public, compressed, {write_concurrency, true}, {read_concurrency, true}])
  ,ecql_statements = ets:new(ecql_statements, [named_table, public, {read_concurrency, true}, {keypos, 2}])
  ,{ok, {}}
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
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
.

%%------------------------------------------------------------------------------
handle_info(timeout, State) ->
   % Who timed out?
   error_logger:error_msg("ecql_etsman: Timeout occured~n")
  ,{noreply, State}
.

%%------------------------------------------------------------------------------
terminate(_Reason, State) ->
  {shutdown, State}
.

%%------------------------------------------------------------------------------
code_change(_ ,State ,_) ->
  {ok ,State}
.

%%==============================================================================
%% END OF FILE
