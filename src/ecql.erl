%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_app.erl - Connector
%%==============================================================================
-module(ecql).
-behaviour(application).

%% Includes
-include("ecql.hrl").

% Compare default settings with CASSANDRA-5727
-define(COMPACTION, "compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 160}").
-define(CL_DEFAULT, ?CL_LOCAL_QUORUM).

%% OTP application
-export([start/2, stop/1]).

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
  case application:get_env(module) of
    {_RWModule, _WriteModule} ->
       {ok, _} = application:ensure_all_started(erlcass)
      ,ecql_sup:start_link()
    ;
    ecql_erlcass ->
       {ok, _} = application:ensure_all_started(erlcass)
      ,ecql_sup:start_link()
    ;
    undefined ->
      ecql_sup:start_link()
    ;
    ecql_native ->
      ecql_sup:start_link()
    %~
  end
.

%%------------------------------------------------------------------------------
stop(_State) ->
  ok
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
config(Key) ->
  ecql_native:config(Key)
.

%%------------------------------------------------------------------------------
config(module, Value) ->
  case validate_module(Value) of
     ok -> ecql_native:config(module, Value);
     Other -> Other
  end
;
config(Key, Value) ->
  ecql_native:config(Key, Value)
.

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
validate_module(ecql_native) -> ok;
validate_module(ecql_erlcass) -> ok;
validate_module({A, {rw, B}}) -> validate_module({A, B});
validate_module({A, {rwv, B}}) -> validate_module({A, B});
validate_module({A, B}) when is_atom(A), is_atom(B) ->
  case validate_module(A) of
    ok -> validate_module(B);
    Other -> Other
  end
;
validate_module(Other) -> {error, {invalid_module_value, Other}}.

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
  case config(module) of
    {RwModule, {rwv, WModule}} ->
       Ret = RwModule:with_stream_do(Function, Args)
      ,case erts_debug:flat_size(Ret) < ecql_replicator2:max_ref_size() of
        true -> ecql_replicator2:forward(Function, Args, {rw, WModule}, Ret);
        false -> ecql_replicator2:forward(Function, Args, {rw, WModule})
      end
      ,Ret
    ;
    {RwModule, WModule} ->
       ecql_replicator2:forward(Function, Args, WModule)
      ,RwModule:with_stream_do(Function, Args)
    ;
    undefined ->
      ecql_native:with_stream_do(Function, Args)
    ;
    Module ->
      Module:with_stream_do(Function, Args)
    %~
  end
.

%%==============================================================================
%% END OF FILE
