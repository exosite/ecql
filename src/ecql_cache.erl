%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_cache.erl - Cache
%%==============================================================================
-module(ecql_cache).
-behaviour(gen_server).

%% Public API
-export([
   clear/0
  ,get/2
  ,dirty/1
  ,set/2
]).

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

%% Defines
-define(CACHE_SIZE, 1000000).
-define(seconds(X), X*1000000).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
clear() ->
  gen_server:abcast(?MODULE, clear)
.

%%------------------------------------------------------------------------------
get(Key, FunResult) ->
  Index = erlang:phash2(Key, ?CACHE_SIZE)
 ,case ets:lookup(?MODULE, Index) of
    [{Index, Key, undef, _}] ->
      set(Key, FunResult())
    ;
    [{Index, Key, Value, Time}] ->
      Diff = timer:now_diff(now(), Time)
     ,case Diff > (?seconds(3600)*24*30) of
        true ->
          set(Key, FunResult())
        ;
        false ->
          Value
        %~
      end
    ;
    _Other -> % [] or [Index, OtherKey, Value, Time]
      set(Key, FunResult())
    %~
  end
.

%%------------------------------------------------------------------------------
dirty(Key) ->
  set(Key, undef)
.

%%------------------------------------------------------------------------------
set(Key, Result) ->
  gen_server:abcast(?MODULE, {set, Key, Result})
 ,Result
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {} ,[])
.

%%------------------------------------------------------------------------------
init(_) ->
   Configuration = application:get_all_env()
  ,ok = net_kernel:monitor_nodes(true)
  ,{ok, Configuration}
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
handle_cast(clear, State) ->
   ets:delete_all_objects(?MODULE)
  ,{noreply, State}
;
handle_cast({set, Key, Result}, State) ->
  Index = erlang:phash2(Key, ?CACHE_SIZE)
 ,ets:insert(?MODULE, {Index, Key, Result, now()})
 ,{noreply, State}
;
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
.

%%------------------------------------------------------------------------------
handle_info({nodeup, _}, State) ->
   % Can ignore
   {noreply, State}
;
handle_info({nodedown, _}, State) ->
   % Nodedown means we likely lost messages
   clear()
  ,{noreply, State}
;
handle_info(timeout, State) ->
   % Who timed out?
   error_logger:error_msg("ecql_cache: Timeout occured~n")
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
