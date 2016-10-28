%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_app.erl - Connector
%%==============================================================================
-module(ecql).
-behaviour(application).
-behaviour(gen_server).

%% Includes
-include("ecql.hrl").

-record(state, {clients = {}, counter = 0, settings, waiting = [], dirty = false}).

-define(DUPLICATE_TABLE, 9216).
-define(DUPLICATE_INDEX, 8704).
% Compare default settings with CASSANDRA-5727
-define(COMPACTION, "compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 160}").
-define(CL_DEFAULT, ?CL_LOCAL_QUORUM).
-define(RECONNECT_INTERVALL, 5000).

%% OTP application
-export([start/2, stop/1]).
-export([start_link/0, stop/0]).

%% OTP gen_server
-export([
   init/1
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
  ,terminate/2
]).

%% Public API
-export([
   config/1
  ,config/2
  ,foldl/3
  ,foldl/4
  ,foldl/5
  ,foldl_page/3
  ,foldl_page/4
  ,foldl_page/5
  ,foreach/2
  ,foreach/3
  ,foreach/4
  ,execute/1
  ,execute/2
  ,execute/3
  ,execute_async/1
  ,execute_async/2
  ,execute_async/3
  ,execute_batch/2
  ,execute_batch/3
  ,eval/1
  ,eval_all/1
  ,quote/1
  ,release/0
  ,select/1
  ,select/2
  ,select/3
  ,select_firstpage/1
  ,select_firstpage/2
  ,select_firstpage/3
  ,select_nextpage/1
  ,select_value/1
  ,select_value/2
  ,select_value/3
  ,select_column/1
  ,select_column/2
  ,select_column/3
  ,select_column/4
  ,sync/0
  ,term_to_bin/1
  ,create_index/3
  ,create_table/2
  ,create_table/3
  ,indexof/2
]).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP application API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start(_Type, _StartArgs) ->
  ecql_sup:start_link()
.

%%------------------------------------------------------------------------------
stop(_State) ->
  ok
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
   process_flag(trap_exit, true)
  ,gen_server:cast(?MODULE, init)
  ,{ok, #state{}}
.
do_init(State) ->
   Configuration = application:get_all_env()
  ,Hosts = proplists:get_value(hosts, Configuration, [])
  ,Configuration1 = do_init_keyspace(Hosts ,Configuration)
  ,Connections = repair_connection_pool({}, Configuration1)
  ,State#state{settings = Configuration1, clients = Connections}
.
do_init_keyspace([] ,Configuration) ->
  receive {config, Key, Value} ->
     Configuration1 = lists:keystore(Key, 1, Configuration, {Key, Value})
  after 1000 ->
     Configuration1 = Configuration
  end
  ,do_init_keyspace(proplists:get_value(hosts, Configuration1, []) ,Configuration1)
;
do_init_keyspace([Host | Hosts] ,Configuration) ->
  case ecql_connection:start_link(Host ,Configuration) of
    {ok, Connection} ->
       Keyspace = proplists:get_value(keyspace, Configuration, "ecql")
      ,Factor = proplists:get_value(replication_factor, Configuration, 2)
      ,Stream = ecql_connection:get_stream(Connection)
      ,Strategy = proplists:get_value(replication_strategy, Configuration, "SimpleStrategy")
      ,CQL = [
         "CREATE KEYSPACE IF NOT EXISTS "
        ,Keyspace
        ," with REPLICATION = {'class':'"
        ,Strategy
        ,"'"
        ,data_centers(Strategy, Factor)
        ,"} "
       ]
      ,log(init_query(Stream, CQL), query, [Stream, CQL])
      ,ok = ecql_connection:stop(Connection)
      ,Configuration
    ;
    Error ->
       error_logger:error_msg("ecql: Failed connecting to: ~p: ~p~n", [Host, Error])
      ,do_init_keyspace(Hosts ,Configuration)
    %~
  end
.

%%------------------------------------------------------------------------------
data_centers("SimpleStrategy", Factor) ->
  [", 'replication_factor':", integer_to_list(Factor)]
;
data_centers("NetworkTopologyStrategy", []) ->
  []
;
data_centers("NetworkTopologyStrategy" = S, [{Name, Factor} | Rest]) ->
  [", '", Name, "':", integer_to_list(Factor) | data_centers(S, Rest)]
.

%%------------------------------------------------------------------------------
repair_connection_pool(OldPool, Configuration) ->
   Count = proplists:get_value(connections_per_host, Configuration, 4)
  ,Hosts = proplists:get_value(hosts, Configuration, [])
  ,list_to_tuple(add_hosts(Hosts, Count, [], tuple_to_list(OldPool), Configuration))
.

%%------------------------------------------------------------------------------
add_hosts([], _Count, NewPool, OldPool, _Configuration) ->
  [ecql_connection:stop(OldConn) ||
     {_, OldConn} <- OldPool
    ,erlang:is_pid(OldConn)
    ,erlang:is_process_alive(OldConn)
  ]
  ,NewPool
;
add_hosts([Host | Hosts], Count, NewPool, OldPool0, Configuration) ->
   {OldConnPool0, OldPool} = lists:partition(fun({OldHost, _}) -> OldHost == Host end, OldPool0)
  ,OldConnPool = [OldConn ||
     {OldHost, OldConn} <- OldConnPool0
    ,OldHost == Host
    ,erlang:is_pid(OldConn)
    ,erlang:is_process_alive(OldConn)
  ]
  ,add_hosts(
     Hosts
    ,Count
    ,add_host(Host, Count, NewPool, OldConnPool, Configuration)
    ,OldPool
    ,Configuration
  )
.

%%------------------------------------------------------------------------------
add_host(_Host, 0, NewPool, OldConnPool, _Configuration) ->
   [ecql_connection:stop(OldConn) || {_, OldConn} <- OldConnPool]
  ,NewPool
;
add_host(Host, N, NewPool0, [], Configuration) ->
   NewPool = add_connection(Host, Configuration, NewPool0)
  ,add_host(Host, N - 1, NewPool, [], Configuration)
;
add_host(Host, N, NewPool, [OldConn | OldConnPool], Configuration) ->
   add_host(Host, N - 1, [{Host, OldConn} | NewPool], OldConnPool, Configuration)
.

%%------------------------------------------------------------------------------
add_connection(Host, Configuration, NewPool) ->
  case ecql_connection:start_link(Host, Configuration) of
    {ok, Connection} ->
       Keyspace = proplists:get_value(keyspace, Configuration, "ecql")
      ,lists:foreach(
         fun(Pid) -> init_query({Host, Pid}, ["USE ", Keyspace]) end
        ,ecql_connection:get_streams(Connection)
      )
      ,[{Host, Connection} | NewPool]
    ;
    Error ->
       Error
      ,NewPool
    %~
  end
.

%%------------------------------------------------------------------------------
init_query(Id, Cql) ->
  ecql_stream:query(Id, Cql, [], ?CL_ONE)
.

%%------------------------------------------------------------------------------
stop() ->
  gen_server:call(?MODULE, stop, infinity)
.

%%------------------------------------------------------------------------------
handle_call(stop, _From, State = #state{clients = Connections}) ->
   [ecql_connection:stop(Connection) || Connection <- tuple_to_list(Connections)]
  ,{stop ,normal ,ok ,State}
;
handle_call({config, Key}, _From, State = #state{settings = Configuration}) ->
   {reply, proplists:get_value(Key, Configuration, undefined), State}
;
handle_call({config, Key, Value}, _From, S = #state{settings = Configuration}) ->
   S1 = S#state{settings = lists:keystore(Key, 1, Configuration, {Key, Value})}
  ,self() ! repair
  ,{reply, ok, S1#state{dirty = true}}
;
handle_call(connection, From, State = #state{clients = {}, waiting = Waiting}) ->
   {noreply, State#state{waiting = [From | Waiting]}}
;
handle_call(connection, _From, State = #state{clients = Connections, counter = Counter}) ->
   ConnectionId = (Counter rem size(Connections)) + 1
  ,{_, Connection} = element(ConnectionId, Connections)
  ,{reply, Connection, State#state{counter=Counter+1}}
.

%%------------------------------------------------------------------------------
handle_cast(init ,State) ->
  {noreply ,do_init(State)}
;
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
.

%%------------------------------------------------------------------------------
handle_info({'EXIT', _Pid, normal}, State) ->
   {noreply, State}
;
handle_info({'EXIT', Pid, Reason}, State = #state{dirty = Dirty, clients = Connections0}) ->
   error_logger:error_msg("ecql: ~p crashed because ~p~n", [Pid, Reason])
  ,Dirty orelse erlang:send_after(?RECONNECT_INTERVALL, self(), repair)
  ,Connections = [Conn || Conn = {_, ConnPid} <- tuple_to_list(Connections0), ConnPid =/= Pid]
  ,{noreply, State#state{dirty = true, clients = list_to_tuple(Connections)}}
;
handle_info(repair, State = #state{settings = Configuration, clients = Connections0, waiting = Waiting}) ->
   Connections = repair_connection_pool(Connections0, Configuration)
  ,Waiting1 = reply(Waiting, Connections, 1)
  ,{noreply, State#state{dirty = false, clients = Connections, waiting = Waiting1}}
;
handle_info(timeout, State) ->
   % Who timed out?
   error_logger:error_msg("ecql: Timeout occured~n")
  ,{noreply, State}
.

%%------------------------------------------------------------------------------
reply([], _, _) ->
  []
;
reply(Waiting, {}, _) ->
  Waiting
;
reply([Client | Clients], Connections, Counter) ->
   ConnectionId = (Counter rem size(Connections)) + 1
  ,{_, Connection} = element(ConnectionId, Connections)
  ,gen_server:reply(Client, Connection)
  ,reply(Clients, Connections, Counter + 1)
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
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
config(Key) ->
  gen_server:call(?MODULE, {config, Key}, infinity)
.

%%------------------------------------------------------------------------------
config(Key, Value) ->
  gen_server:call(?MODULE, {config, Key, Value}, infinity)
.

%%------------------------------------------------------------------------------
%%  Params:
%%    Fun = fun(Row, Acc) -> ... end
%%------------------------------------------------------------------------------
foldl(Fun, Acc, Cql) ->
  foldl(Fun, Acc, Cql, [], ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
foldl(Fun, Acc, Cql, Args) ->
  foldl(Fun, Acc, Cql, Args, ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
foldl(Fun, Acc, Cql, Args, Consistency) ->
  Fun1 = fun(_Keys, Rows, Acc0) ->
    lists:foldl(Fun, Acc0, Rows)
  end
  ,foldl_page(Fun1, Acc, Cql, Args, Consistency)
.

%%------------------------------------------------------------------------------
%%  Params:
%%    Fun = fun(Keys, Rows, Acc) -> ... end
%%------------------------------------------------------------------------------
foldl_page(Fun, Acc, Cql) ->
  foldl_page(Fun, Acc, Cql, [], ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
foldl_page(Fun, Acc, Cql, Args) ->
  foldl_page(Fun, Acc, Cql, Args, ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
foldl_page(Fun, Acc, Cql, Args, Consistency) ->
  with_stream_do(foldl, [Fun, Acc, Cql, Args, Consistency])
.

%%------------------------------------------------------------------------------
foreach(Fun, Cql) ->
  foreach(Fun, Cql, [], ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
foreach(Fun, Cql, Args) ->
  foreach(Fun, Cql, Args, ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
foreach(Fun, Cql, Args, Consistency) ->
  Fun1 = fun(Row, Acc0) ->
     Fun(Row)
    ,Acc0
  end
  ,foldl(Fun1, ok, Cql, Args, Consistency)
.

%%------------------------------------------------------------------------------
release() ->
  case get(last_ccon) of
     undefined ->
      ok
    ;
    _ ->
       Ret = with_stream_do(release, [])
      ,erase(last_ccon)
      ,Ret
    %~
  end
.

%%------------------------------------------------------------------------------
select_value(Cql) ->
  select_value(Cql, [], ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
select_value(Cql, Args) ->
  select_value(Cql, Args, ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
select_value(Cql, Args, Consistency) ->
  case select_column(Cql, 1, Args ,Consistency) of
    [] ->
      undefined
    ;
    List ->
      hd(List)
    %~
  end
.

%%------------------------------------------------------------------------------
select_firstpage(Cql) ->
  select_firstpage(Cql, [], ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
select_firstpage(Cql, Args) ->
  select_firstpage(Cql, Args, ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
select_firstpage(Cql, Args, Consistency) ->
  with_stream_do(query_page, [Cql, Args, Consistency])
.

%%------------------------------------------------------------------------------
select_nextpage(Continuation) ->
  with_stream_do(query_page, [Continuation])
.

%%------------------------------------------------------------------------------
select_column(Cql) ->
  select_column(Cql, 1, [], ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
select_column(Cql, Col) ->
  select_column(Cql, Col, [], ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
select_column(Cql, Col, Args) ->
  select_column(Cql, Col, Args, ?CL_DEFAULT)
.

%%------------------------------------------------------------------------------
select_column(Cql, Col, Args, Consistency) ->
   {_Keys, Rows} = execute(Cql, Args, Consistency)
  ,[lists:nth(Col, Row) || Row <- Rows]
.

%%------------------------------------------------------------------------------
select(Cql) ->
  execute(Cql, [] ,?CL_DEFAULT)
.
select(Cql, Args) ->
  execute(Cql, Args ,?CL_DEFAULT)
.
select(Cql, Args ,Consistency) ->
  execute(Cql, Args ,Consistency)
.

%%------------------------------------------------------------------------------
sync() ->
  with_stream_do(sync, [])
.

%%------------------------------------------------------------------------------
execute(Cql) ->
  execute(Cql, [])
.
execute(Cql, Args) ->
  execute(Cql, Args, ?CL_DEFAULT)
.
execute(Cql, Args, Consistency) ->
  with_stream_do(query, [Cql, Args, Consistency])
.

%%------------------------------------------------------------------------------
execute_async(Cql) ->
  execute_async(Cql, [])
.
execute_async(Cql, Args) ->
  execute_async(Cql, Args, ?CL_DEFAULT)
.
execute_async(Cql, Args, Consistency) ->
  with_stream_do(query_async, [Cql, Args, Consistency])
.

%%------------------------------------------------------------------------------
execute_batch(Cql, ListOfArgs) ->
  execute_batch(Cql, ListOfArgs, ?CL_DEFAULT)
.
execute_batch(_Cql, [], _Consistency) ->
  ok
;
execute_batch(Cql, ListOfArgs, Consistency) ->
  with_stream_do(query_batch, [Cql, ListOfArgs, Consistency])
.

%%------------------------------------------------------------------------------
create_index(Indexname, Tablename, Columnname) ->
  with_stream_do(query, [[
     "CREATE INDEX IF NOT EXISTS ", Indexname, " ON ", Tablename
    ," (", Columnname, ");"
  ], [], ?CL_ONE])
.

%%------------------------------------------------------------------------------
create_table(Tablename, TableDef) ->
  with_stream_do(query, [[
     "CREATE TABLE IF NOT EXISTS ", Tablename, " ( ", TableDef, " ) WITH "
    ,?COMPACTION
    ,";"
  ], [], ?CL_ONE])
.

%%------------------------------------------------------------------------------
create_table(Tablename, TableDef, Comment) ->
  with_stream_do(query, [[
     "CREATE TABLE IF NOT EXISTS ", Tablename, " ( ", TableDef, " ) WITH "
    ,?COMPACTION
    ," AND comment='", Comment, "';"
  ], [], ?CL_ONE])
.

%%------------------------------------------------------------------------------
eval(Binary) when is_binary(Binary) ->
  binary_to_term(Binary)
.

%%------------------------------------------------------------------------------
eval_all(Values) ->
  [eval(Value) || Value <- Values, Value =/= undefined]
.

%%------------------------------------------------------------------------------
term_to_bin(Value) ->
  term_to_binary(Value, [{compressed, 6}])
.

%%------------------------------------------------------------------------------
quote(Integer) when is_integer(Integer) ->
  integer_to_list(Integer)
;
quote(List) ->
  [$', escape(List), $']
.


%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Private API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
escape(List) ->
  [
    case Char of
      $' -> "''";
      _ -> Char
    end
    || Char <- lists:flatten(List)
  ]
.

%%------------------------------------------------------------------------------
% This indexof fails on not found
indexof(Element, [Element | _]) ->
  0
;
indexof(Element, [_ | Tail]) ->
  indexof(Element, Tail) + 1
.

%%------------------------------------------------------------------------------
with_stream_do(Function, Args) ->
  Stream = case get(last_ccon) of
    undefined ->
       Connection = gen_server:call(?MODULE, connection, infinity)
      ,Stream0 = ecql_connection:get_stream(Connection)
      ,put(last_ccon, Stream0)
      ,Stream0
    ;
    LastStream ->
       LastStream
    %~
  end
  ,try log(apply(ecql_stream, Function, [Stream | Args]), Function, Args)
   catch
     exit:{noproc, _} ->
       put(last_ccon, undefined)
      ,with_stream_do(Function, Args)
     %~
   end
.

%%------------------------------------------------------------------------------
log({error, Code, Message} = Ret, Function, Args) ->
  error_logger:error_msg(
     "ecql_stream:~p(~p) failed: {error, ~p, ~s}~n"
    ,[Function, Args, Code, Message]
  )
  ,Ret
;
log(Ret, _Function, _Args) ->
  Ret
.


%%==============================================================================
%% END OF FILE
