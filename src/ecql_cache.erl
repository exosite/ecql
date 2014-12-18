%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_cache.erl - Cache
%%==============================================================================
-module(ecql_cache).
-behaviour(gen_server).

%% Public API
-export([
   cache_size/0
  ,clear/0
  ,get/2
  ,dirty/1
  ,set/2
  ,set_cache_size/1
  ,stats/0
  ,clear_stats/0
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
%-define(stats, false).
-define(DEFAULT_CACHESIZE, 1000000).
-define(seconds(X), X*1000000).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
cache_size() ->
  case ets:lookup(?MODULE, cache_size) of
    [{cache_size, CacheSize}] ->
      CacheSize
    ;
    _Other ->
      ?DEFAULT_CACHESIZE
    %~
  end
.

%%------------------------------------------------------------------------------
clear() ->
  gen_server:abcast(?MODULE, clear)
.

%%------------------------------------------------------------------------------
get(Key, FunResult) ->
  Index = erlang:phash2(Key, cache_size())
 ,case ets:lookup(?MODULE, Index) of
    [{Index, Key, undef, _}] ->
       incr_stat(undef)
      ,set(Key, FunResult())
    ;
    [{Index, Key, Value, Time}] ->
      Diff = timer:now_diff(now(), Time)
     ,case Diff > (?seconds(3600)*24*30) of
        true ->
           incr_stat(old)
          ,set(Key, FunResult())
        ;
        false ->
          Value
        %~
      end
    ;
    [] -> % [] or [Index, OtherKey, Value, Time]
       incr_stat(empty)
      ,set(Key, FunResult())
    ;
    _Other -> % [] or [Index, OtherKey, Value, Time]
       incr_stat(conflict)
      ,set(Key, FunResult())
    %~
  end
.

%%------------------------------------------------------------------------------
get_stat(Key) ->
  case ets:lookup(?MODULE, Key) of
    [{Key, Num}] ->
      Num
    ;
    _Other ->
      0
    %~
  end
.

%%------------------------------------------------------------------------------
stats() ->
  [{Key, get_stat(Key)} || Key <- [undef, empty, old, conflict]]
.

%%------------------------------------------------------------------------------
clear_stats() ->
  [set_stat(Key, 0) || {Key, _} <- stats()]
.

%%------------------------------------------------------------------------------
-ifdef(stats).
incr_stat(Key) ->
  set_stat(Key, get_stat(Key) + 1)
.
-else.
incr_stat(_Key) ->
  ok
.
-endif.

%%------------------------------------------------------------------------------
set_stat(Key, Num) when is_integer(Num) ->
  ets:insert(?MODULE, {Key, Num})
.


%%------------------------------------------------------------------------------
dirty(Key) ->
   gen_server:abcast(nodes(), ?MODULE, {dirty, Key})
  ,do_dirty(Key)
.
do_dirty(Key) ->
  Index = erlang:phash2(Key, cache_size())
 ,case ets:lookup(?MODULE, Index) of
    [] ->
      ok
    ;
    [{Index, Key, _, _}] ->
      ets:insert(?MODULE, {Index, Key, undef, now()})
    ;
    _Other ->
      ok
    %~
  end
.

%%------------------------------------------------------------------------------
set(Key, Result) ->
  Index = erlang:phash2(Key, cache_size())
 ,ets:insert(?MODULE, {Index, Key, Result, now()})
 ,Result
.

%%------------------------------------------------------------------------------
set_cache_size(CacheSize) when is_integer(CacheSize) ->
   ets:delete_all_objects(?MODULE)
  ,ets:insert(?MODULE, {cache_size, CacheSize})
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
  ,set_cache_size(proplists:get_value(cache_size, Configuration, ?DEFAULT_CACHESIZE))
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
   CacheSize = cache_size()
  ,ets:delete_all_objects(?MODULE)
  ,set_cache_size(CacheSize)
  ,{noreply, State}
;
handle_cast({dirty, Key}, State) ->
  do_dirty(Key)
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
