%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_app.erl - Connector
%%==============================================================================
-module(ecql).
-behaviour(application).
-behaviour(gen_server).

-record(state, {clients, counter = 0, settings}).

-define(DUPLICATE_TABLE, 9216).
-define(DUPLICATE_INDEX, 8704).

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
   execute/1
  ,execute/2
  ,execute_async/1
  ,execute_async/2
  ,execute_batch/2
  ,eval/1
  ,eval_all/1
  ,quote/1
  ,select/1
  ,select/2
  ,select_value/1
  ,select_value/2
  ,select_column/1
  ,select_column/2
  ,select_column/3
  ,term_to_bin/1
  ,create_once/1
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
   % Sleeping to calm down fast restarts on crash
   timer:sleep(1000)
  ,ecql_statements = ets:new(ecql_statements, [named_table, public, {write_concurrency, true}, {read_concurrency, true}])
  ,Configuration = application:get_all_env()
  ,init_keyspace(Configuration)
  ,Count = proplists:get_value(connections, Configuration, 4)
  ,Connections = list_to_tuple(init_connection_pool(Count, Configuration))
  ,{ok, #state{clients = Connections, settings = Configuration}}
.
init_keyspace(Configuration) ->
   {ok, Connection} = ecql_connection:start_link(Configuration)
  ,Keyspace = proplists:get_value(keyspace, Configuration, "ecql")
  ,Factor = proplists:get_value(replication_factor, Configuration, 2)
  ,Stream = ecql_connection:get_stream(Connection)
  ,Strategy = proplists:get_value(replication_strategy, Configuration, "SimpleStrategy")
  ,CQL = [
     "CREATE KEYSPACE "
    ,Keyspace
    ," with REPLICATION = {'class':'"
    ,Strategy
    ,"'"
    ,data_centers(Strategy, Factor)
    ,"} "
   ]
  ,accept_duplicate(ecql_stream:query(Stream, CQL))
  ,ok = ecql_connection:stop(Connection)
.
data_centers("SimpleStrategy", Factor) ->
  [", 'replication_factor':", integer_to_list(Factor)]
;
data_centers("NetworkTopologyStrategy", []) ->
  []
;
data_centers("NetworkTopologyStrategy" = S, [{Name, Factor} | Rest]) ->
  [", '", Name, "':", integer_to_list(Factor) | data_centers(S, Rest)]
.

init_connection_pool(0, _) ->
  []
;
init_connection_pool(N, Configuration) ->
  [init_connection(Configuration)  | init_connection_pool(N-1, Configuration)]
.
init_connection(Configuration) ->
   {ok, Connection} = ecql_connection:start_link(Configuration)
  ,Keyspace = proplists:get_value(keyspace, Configuration, "ecql")
  ,lists:foreach(
     fun(Stream0) -> ecql_stream:query(Stream0, ["USE ", Keyspace]) end
    ,ecql_connection:get_streams(Connection)
   )
  ,Connection
.



%%------------------------------------------------------------------------------
stop() ->
  gen_server:call(?MODULE, stop)
.

%%------------------------------------------------------------------------------
handle_call(stop, _From, State = #state{clients = Connections}) ->
   [ecql_connection:stop(Connection) || Connection <- tuple_to_list(Connections)]
  ,{stop ,normal ,ok ,State}
;
handle_call(connection, _From, State = #state{clients = Connections, counter = Counter}) ->
   ConnectionId = (Counter rem size(Connections)) + 1
  ,{reply, element(ConnectionId, Connections), State#state{counter=Counter+1}}
.

%%------------------------------------------------------------------------------
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
.

%%------------------------------------------------------------------------------
handle_info(timeout, State) ->
   % Who timed out?
   error_logger:error_msg("ecql_connection: Timeout occured~n")
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


%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
select_value(Cql) ->
  select_value(Cql, [])
.

%%------------------------------------------------------------------------------
select_value(Cql, Args) ->
  case select_column(Cql, 1, Args) of
    [] ->
      undefined
    ;
    List ->
      hd(List)
    %~
  end
.

%%------------------------------------------------------------------------------
select_column(Cql) ->
  select_column(Cql, 1, [])
.

%%------------------------------------------------------------------------------
select_column(Cql, Col) ->
  select_column(Cql, Col, [])
.

%%------------------------------------------------------------------------------
select_column(Cql, Col, Args) ->
   {_Keys, Rows} = execute(Cql, Args)
  ,[lists:nth(Col, Row) || Row <- Rows]
.

%%------------------------------------------------------------------------------
select(Cql) ->
  select(Cql, [])
.
select(Cql, Args) ->
  execute(Cql, Args)
.

%%------------------------------------------------------------------------------
execute(Cql) ->
  execute(Cql, [])
.
execute(Cql, Args) ->
  accept_ok(ecql_stream:query(anystream(), Cql, Args))
.

%%------------------------------------------------------------------------------
execute_async(Cql) ->
  execute_async(Cql, [])
.
execute_async(Cql, Args) ->
  accept_ok(ecql_stream:query_async(anystream(), Cql, Args))
.

%%------------------------------------------------------------------------------
execute_batch(Cql, ListOfArgs) ->
  accept_ok(ecql_stream:query_batch(anystream(), Cql, ListOfArgs))
.

%%------------------------------------------------------------------------------
create_once(Cql) ->
  accept_duplicate(ecql_stream:query(anystream(), Cql))
.

%%------------------------------------------------------------------------------
create_table(Tablename, Body) ->
  create_once([
     "CREATE TABLE ", Tablename, " ( ", Body, " ) WITH "
    ," compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 20} AND caching = 'all' "
    ,";"
  ])
.

%%------------------------------------------------------------------------------
create_table(Tablename, Body, Comment) ->
  create_once([
     "CREATE TABLE ", Tablename, " ( ", Body, " ) WITH "
    ," compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 20} AND caching = 'all' "
    ," AND comment='", Comment, "';"
  ])
.

%%------------------------------------------------------------------------------
accept_duplicate({error, ?DUPLICATE_TABLE, _Message}) ->
  ok
;
accept_duplicate({error, ?DUPLICATE_INDEX, _Message}) ->
  ok
;
accept_duplicate(Other) ->
  accept_ok(Other)
.

%%------------------------------------------------------------------------------
accept_ok({error, Code, Message} = Ret) ->
   error_logger:error_msg("query failed: {error, ~p, ~s}~n", [Code, Message])
  ,Ret
;
accept_ok(Ret) ->
  Ret
.

%%------------------------------------------------------------------------------
eval(Binary) when is_binary(Binary) ->
  binary_to_term(Binary)
.

%%------------------------------------------------------------------------------
eval_all(Values) ->
  [eval(Value) || Value <- Values]
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
anystream() ->
  case get(last_ccon) of
    undefined ->
       Connection = gen_server:call(?MODULE, connection, 10000)
      ,Stream = ecql_connection:get_stream(Connection)
      ,put(last_ccon, Stream)
      ,Stream
    ;
    LastStream ->
      LastStream
    %~
  end
.

%%==============================================================================
%% END OF FILE
