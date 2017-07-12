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
  ,cluster_module/0
  ,clear/0
  ,get/2
  ,dirty/1
  ,local_match_clear/1
  ,match_clear/1
  ,set/2
  ,set_cache_size/1
  ,set_cluster_module/1
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

%% Includes
-include("ecql.hrl").

%% Defines
%-define(stats, false).
-define(DEFAULT_CACHESIZE, 1000000).
-define(DEFAULT_CLUSTER_MODULE, erlang).
-define(seconds(X), X*1000000).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
cache_size() ->
  private_get(cache_size, ?DEFAULT_CACHESIZE)
.

%%------------------------------------------------------------------------------
cluster_module() ->
  private_get(cluster_module, ?DEFAULT_CLUSTER_MODULE)
.

%%------------------------------------------------------------------------------
current_slice() ->
  element(private_get(current_slice, 1), ?CACHE_SLICES_TUPLE)
.

%%------------------------------------------------------------------------------
clear() ->
  gen_server:abcast(?MODULE, clear)
.

%%------------------------------------------------------------------------------
get(Key, FunResult) ->
  case find(Key) of
    {_Slice, Value} ->
      Value
    ;
    {_Slice, dirty, _} ->
      do_get(Key, FunResult)
    ;
    undefined ->
      do_get(Key, FunResult)
    %~
  end
.
do_get(Key, FunResult) ->
  do_get(Key, FunResult, 1)
.
do_get(Key, FunResult, 10) ->
  incr_stat(empty),
  Result = FunResult(),
  do_set(Key, Result),
  dirty(Key)
;
do_get(Key, FunResult, AttemptCount) ->
  incr_stat(empty),
  Ts = erlang:system_time(micro_seconds),
  Result = FunResult(),
  case do_set(Key, Result, Ts) of
    {ok, Value} -> Value;
    {error, read_again} -> do_get(Key, FunResult, AttemptCount + 1)
  end
.

%%------------------------------------------------------------------------------
get_stat(Key) ->
  private_get(Key, 0)
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
  private_set(Key, Num)
.


%%------------------------------------------------------------------------------
dirty(Key) ->
   Module = cluster_module()
  ,gen_server:abcast(Module:nodes(), ?MODULE, {dirty, Key})
  ,do_dirty(Key)
  ,ok
.
do_dirty(Key) ->
  Ts = erlang:system_time(micro_seconds),
  Record = {Key, dirty, Ts},
  case find(Key) of
    {Slice, dirty, _} ->
      ets:insert(Slice, Record)
    ;
    {Slice, _Value} ->
      ets:insert(Slice, Record)
    ;
    undefined ->
      cache_insert_new(Record)
    %~
  end
.

%%------------------------------------------------------------------------------
local_match_clear(Pattern) when is_atom(Pattern); is_tuple(Pattern) ->
  lists:foreach(
    fun (Slice) ->
      case catch ets:match_delete(Slice, Pattern) of
        {'EXIT', Error} ->
           error_logger:error_msg(
             "~p: local_match_clear error. pattern: ~p, error: ~p~n"
            ,[?MODULE, Pattern, Error]
           )
        ;
        _ ->
           true
        %~
      end
    end
   ,?CACHE_SLICES_LIST
  )
;
local_match_clear(_) ->
  ok
.

%%------------------------------------------------------------------------------
match_clear(Pattern) ->
   ClusterModule = cluster_module()
  ,gen_server:abcast(ClusterModule:nodes(), ?MODULE, {match_clear, Pattern})
  ,local_match_clear(Pattern)
.

%%------------------------------------------------------------------------------
set(Key, Result) ->
  case find(Key) of
    {Slice, dirty, _} ->
       ets:insert(Slice, {Key, Result})
      ,Result
    ;    
    {Slice, _Value} ->
       ets:insert(Slice, {Key, Result})
      ,Result
    ;
    undefined ->
      do_set(Key, Result)
    %~
  end
.
do_set(Key, Result, Timestamp) ->
  case find(Key) of
    {_Slice, dirty, DirtyTimestamp} ->
      io:format("dirty~n"),
      if DirtyTimestamp > Timestamp ->
        io:format("read again~n"),
        {error, read_again}
      ;
      true ->
        io:format("dirty but result is newer than dirty timestamp~n"),
        {ok, do_set(Key, Result)}
      end
    ;
    O ->
      io:format("other ~p~n",[O]),
      {ok, do_set(Key, Result)}
  end
.
do_set(Key, Result) ->
   cache_insert_new({Key, Result})
  ,Result
.

cache_insert_new(Object) ->
   Slice = current_slice()
  ,ets:insert(Slice, Object)
  ,Size = ets:info(Slice, size)
  ,SliceCount = tuple_size(?CACHE_SLICES_TUPLE)
  ,Limit = cache_size() / SliceCount
  ,(Size > Limit) andalso begin
     Index = private_get(current_slice, 1) + 1
    ,case (Index > SliceCount) of
      true ->
        private_set(current_slice, 1)
      ;
      false ->
        private_set(current_slice, Index)
      %~
    end
    ,ets:delete_all_objects(current_slice())
  end
.
%%------------------------------------------------------------------------------
set_cache_size(CacheSize) when is_integer(CacheSize) ->
  private_set(cache_size, CacheSize)
.

%%------------------------------------------------------------------------------
set_cluster_module(Module) when is_atom(Module) ->
  private_set(cluster_module, Module)
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
  ,set_cluster_module(proplists:get_value(cluster_module, Configuration, ?DEFAULT_CLUSTER_MODULE))
  ,{ok, Configuration}
.

%%------------------------------------------------------------------------------
stop() ->
  gen_server:call(?MODULE, stop)
.

%%------------------------------------------------------------------------------
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}
;
handle_call(_, _From, State) ->
  {reply, unknown, State}
.

%%------------------------------------------------------------------------------
handle_cast(clear, State) ->
   CacheSize = cache_size()
  ,Module = cluster_module()
  ,ets:delete_all_objects(?MODULE)
  ,[ets:delete_all_objects(Table) || Table <- ?CACHE_SLICES_LIST]
  ,set_cache_size(CacheSize)
  ,set_cluster_module(Module)
  ,{noreply, State}
;
handle_cast({dirty, Key}, State) ->
  do_dirty(Key)
 ,{noreply, State}
;
handle_cast({match_clear, Pattern}, State) ->
  local_match_clear(Pattern)
 ,{noreply, State}
;
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
;
handle_cast(_, State) ->
  {noreply, State}
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
;
handle_info(_, State) ->
   {noreply, State}
.

%%------------------------------------------------------------------------------
terminate(_Reason, State) ->
  {shutdown, State}
.

%%------------------------------------------------------------------------------
code_change(_ ,State ,_) ->
  {ok ,State}
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Private API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
find(Key) ->
  find(Key, ?CACHE_SLICES_LIST)
.

%%------------------------------------------------------------------------------
find(Key, [Slice | Rest]) ->
  case ets:lookup(Slice, Key) of
    [{Key, Value}] ->
      {Slice, Value}
    ;
    [{Key, dirty, Timestamp}] ->
      {Slice, dirty, Timestamp}
    ;
    [] -> % [] or [Index, OtherKey, Value, Time]
      find(Key, Rest)
    %~
  end
;
find(_Key, []) ->
  undefined
.

%%------------------------------------------------------------------------------
private_get(Key, Default) ->
  case ets:lookup(?MODULE, Key) of
    [{Key, Value}] ->
      Value
    ;
    _Other ->
      Default
    %~
  end
.

%%------------------------------------------------------------------------------
private_set(Key, Value) ->
  ets:insert(?MODULE, {Key, Value})
.


%%==============================================================================
%% END OF FILE
