%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_mnesia.erl - Mnesia drop-in replacement
%%==============================================================================
-module(ecql_mnesia).

-include("ecql.hrl").

%% Mnesia APIs
-export([
   add_table_copy/3
  ,all_keys/1
  ,change_config/2
  ,change_table_copy_type/3
  ,clear_table/1
  ,create_table/2
  ,delete/1, delete/3
  ,delete_object/1
  ,delete_table/1
  ,dirty_all_keys/1
  ,dirty_delete/1, dirty_delete/3
  ,dirty_delete_object/1
  ,dirty_index_match_object/3, dirty_index_match_object/2
  ,dirty_index_read/3
  ,dirty_match_object/1, dirty_match_object/2, dirty_match_object/3
  ,dirty_read/2, dirty_read/3
  ,dirty_select/1, dirty_select/2, dirty_select/4
  ,dirty_update_counter/3
  ,dirty_write/1
  ,first/1
  ,foldr/3
  ,index_match_object/3, index_match_object/2
  ,index_read/3
  ,load_textfile/1
  ,match_object/1, match_object/2, match_object/3
  ,next/2
  ,read/2, read/3
  ,select/1
  ,select/2
  ,select/4
  ,start/0
  ,stop/0
  ,system_info/1
  ,table_info/2
  ,transaction/1
  ,wait_for_tables/2
  ,write/1
]).

%% Extensions Updating (from ets)
-export([
   update_element/3
]).


%% Extensions Counting
-export([
   count/2
  ,dirty_count/2
  ,dirty_index_count/3
  ,dirty_index_match_count/3, dirty_index_match_count/2
  ,dirty_match_count/1, dirty_match_count/2
  ,index_count/3
  ,index_match_count/3, index_match_count/2
  ,match_count/1, match_count/2
]).

%% Extensions Finding (only prim key)
-export([
   dirty_index_key/3
  ,dirty_index_match_key/3, dirty_index_match_key/2
  ,dirty_key/2
  ,dirty_match_key/1, dirty_match_key/2
  ,index_key/3
  ,index_match_key/3, index_match_key/2
  ,key/2
  ,match_key/1, match_key/2
]).

%% Extensions Traversing (inspired by gb_trees)
-export([
   iterator/1
  ,next/1
]).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Mnesia API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start()  ->
  {ok, _Apps} = application:ensure_all_started(ecql)
.

%%------------------------------------------------------------------------------
stop()  ->
  application:stop(ecql)
.

%%------------------------------------------------------------------------------
transaction(Fun)  ->
  try Fun() of
    Value ->
       {atomic ,Value}
    %~
  catch
    Throw -> {aborted , Throw}
    %~
  end
.

%%------------------------------------------------------------------------------
% Returns list of records
dirty_index_read(RecordName, KeyValue, KeyIndex) ->
  index_read(RecordName, KeyValue, KeyIndex)
.
index_read(RecordName, KeyValue, 2) ->
  ecql_cache:get({RecordName, KeyValue}, fun() ->
    do_index_read(RecordName, KeyValue, 2)
  end)
;
index_read(RecordName, KeyValue, KeyIndex) when is_binary(KeyValue) or is_atom(KeyValue) ->
  ecql_cache:get({RecordName, KeyValue, KeyIndex}, fun() ->
    do_index_read(RecordName, KeyValue, KeyIndex)
  end)
;
index_read(RecordName, KeyValue, KeyIndex) ->
  do_index_read(RecordName, KeyValue, KeyIndex)
.
do_index_read(RecordName, KeyValue, KeyIndex) when is_atom(RecordName) ->
  % KeyIndex is actually the list position (starting at 1) behind the RecordName
  % so we need to subtract 2 (1 for starting at one + 1 for skipping the record)
   Index = KeyIndex - 2
  ,select_records(RecordName, [
     "SELECT * FROM ", map_recordname(RecordName)
    ," WHERE ", map_fieldindex(Index), " = ?"
   ], [ecql:term_to_bin(KeyValue)])
.

%%------------------------------------------------------------------------------
dirty_read(RecordName, KeyValue) ->
  read(RecordName, KeyValue)
.
read(RecordName, KeyValue) ->
  index_read(RecordName, KeyValue, 2)
.

%%------------------------------------------------------------------------------
dirty_read(RecordName, KeyValue, _Lock) ->
  read(RecordName, KeyValue)
.
read(RecordName, KeyValue, _Lock) ->
  read(RecordName, KeyValue)
.

%%------------------------------------------------------------------------------
dirty_write(Record)  ->
  write(Record)
.
write(Record) when is_tuple(Record) ->
   dirty([RecordName | RecordValues] = tuple_to_list(Record))
  ,do_update_element(RecordName, lists:seq(2, tuple_size(Record)), RecordValues)
.

%%------------------------------------------------------------------------------
dirty_index_match_object(_RecordName, RecordPattern, _KeyIndex) ->
  match_object(RecordPattern)
.
index_match_object(_RecordName, RecordPattern, _KeyIndex) ->
  match_object(RecordPattern)
.

%%------------------------------------------------------------------------------
dirty_index_match_object(RecordPattern, _KeyIndex) ->
  match_object(RecordPattern)
.
index_match_object(RecordPattern, _KeyIndex) ->
  match_object(RecordPattern)
.

%%------------------------------------------------------------------------------
dirty_match_object(_RecordName, RecordPattern, _LockKind) ->
  match_object(_RecordName, RecordPattern, _LockKind)
.
match_object(_RecordName, RecordPattern, _LockKind) ->
  match_object(RecordPattern)
.

%%------------------------------------------------------------------------------
dirty_match_object(_RecordName, RecordPattern) ->
  match_object(RecordPattern)
.
match_object(_RecordName, RecordPattern) ->
  match_object(RecordPattern)
.

%%------------------------------------------------------------------------------
dirty_match_object(RecordPattern) ->
  match_object(RecordPattern)
.
match_object(RecordPattern) when is_tuple(RecordPattern) ->
  match_object(tuple_to_list(RecordPattern))
;
match_object([RecordName | RecordValues]) when is_atom(RecordName) ->
  MatchValues = [
    {ecql:indexof(Value, RecordValues)+2, Value}
    || Value <- RecordValues, not is_ref(Value)
  ]
 ,case MatchValues of
    [{KeyIndex, KeyValue}] ->
      index_read(RecordName, KeyValue, KeyIndex)
    ;
    _ ->
      {Keys, Values} = and_pairs(RecordValues)
      ,select_records(RecordName, [
         "SELECT * FROM ", map_recordname(RecordName)
        ," WHERE ", Keys
        ," ALLOW FILTERING;"
       ], Values)
    %~
  end
.

%%------------------------------------------------------------------------------
dirty_delete(RecordName, KeyValue, Lock) ->
  delete(RecordName ,KeyValue ,Lock)
.
delete(RecordName, KeyValue, _Lock) ->
  delete({RecordName, KeyValue})
.

%%------------------------------------------------------------------------------
dirty_delete({RecordName, KeyValue}) ->
  delete({RecordName, KeyValue})
.
delete({RecordName, KeyValue}) when is_atom(RecordName) ->
  delete_object(read(RecordName, KeyValue))
.

%%------------------------------------------------------------------------------
dirty_delete_object(Records) ->
  delete_object(Records)
.
delete_object(Records) when is_list(Records) ->
   lists:foreach(fun delete_object/1, Records)
  ,ok
;
delete_object(Record) when is_tuple(Record) ->
  % Correct Impl?
  % Should delete all object according to the pattern or only based on prim key?
   dirty([RecordName | RecordValues] = tuple_to_list(Record))
  ,{RecordName, Table} = lists:keyfind(RecordName, 1, get_tables())
  ,{type, Type} = lists:keyfind(type, 1, Table)
  ,do_delete_object(Type, RecordName, RecordValues)
.
do_delete_object(set, RecordName, RecordValues) ->
   ecql:execute(
     ["DELETE FROM ", map_recordname(RecordName), " WHERE a = ?"]
    ,[ecql:term_to_bin(hd(RecordValues))]
   )
;
do_delete_object(bag, RecordName, RecordValues) ->
   {Keys, Values} = and_pairs(RecordValues)
  ,ecql:execute([
     "DELETE FROM ", map_recordname(RecordName)
    ," WHERE ", Keys
  ], Values)
.

%%------------------------------------------------------------------------------
% REALLY DONT!
dirty_all_keys(RecordName) ->
  all_keys(RecordName)
.
all_keys(RecordName) when is_atom(RecordName) ->
  ecql:eval_all(ecql:select_column([
    "SELECT a FROM ", map_recordname(RecordName)
  ]))
.

%%------------------------------------------------------------------------------
first(RecordName) when is_atom(RecordName) ->
  key_or_end(firstn(RecordName, 1))
.

%%------------------------------------------------------------------------------
next(RecordName, KeyValue) when is_atom(RecordName) ->
  key_or_end(nextn(RecordName, KeyValue, 1))
.

%%------------------------------------------------------------------------------
wait_for_tables([], _Timeout) ->
  ok
;
wait_for_tables(Tables, infinity) ->
   NewTables = lists:subtract(Tables, system_info(tables))
  ,do_wait_for_tables(NewTables, infinity)
;
wait_for_tables(Tables, Number) ->
   NewTables = lists:subtract(Tables, system_info(tables))
  ,do_wait_for_tables(NewTables, Number - 100)
.

do_wait_for_tables([], _Number) ->
   ok
;
do_wait_for_tables(Tables, Number) when Number < 0 ->
   {timeout, Tables}
;
do_wait_for_tables(Tables, Number) ->
   timer:sleep(100)
  ,wait_for_tables(Tables, Number)
.

%%------------------------------------------------------------------------------
system_info(tables) ->
   {_, Tables} = ecql:select([
     "SELECT columnfamily_name, comment FROM  system.schema_columnfamilies"
   ])
  ,[
    unmap_recordname(TableName)
    || [TableName, Comment] <- Tables
      ,string:left(Comment, 11) =:= "ecql_mnesia"
   ]
;
system_info(use_dir) ->
  false
.

%%------------------------------------------------------------------------------
% NOP
change_table_copy_type(_Schema,_This,_DiscCopies) ->
  {atomic ,ok}
.

%%------------------------------------------------------------------------------
clear_table(RecordName) when is_atom(RecordName) ->
  case ecql:execute(["TRUNCATE ", map_recordname(RecordName)]) of
     ok -> {atomic ,ok}
    ;Error -> {error, Error}
  end
.

%%------------------------------------------------------------------------------
delete_table(RecordName) when is_atom(RecordName) ->
  case ecql:execute(["DROP TABLE ", map_recordname(RecordName)]) of
     ok -> {atomic ,ok}
    ;Error -> {error, Error}
  end
.

%%------------------------------------------------------------------------------
% NOP
table_info(_Table ,where_to_commit) ->
  [{nop ,nop}]
.

%%------------------------------------------------------------------------------
create_table(Name ,Def) ->
   {type, Type} = lists:keyfind(type, 1, Def)
  ,{attributes, Fields} = lists:keyfind(attributes, 1, Def)
  ,ok = do_create_table(Type, Name, Fields)
  ,case lists:keyfind(index, 1, Def) of
    {index, Indexes} ->
       lists:foreach(
         fun(IndexName) ->
           Index = ecql:indexof(IndexName, Fields)
          ,ok = ecql:create_index(
             map_recordname(Name) ++ "_" ++ map_recordname(IndexName)
            ,map_recordname(Name)
            ,map_fieldindex(Index)
          )
         end
        ,Indexes
       )
    ;
    false ->
       ok
    %~
   end
  ,ok = ecql:config(tables, undefined)
  ,{atomic ,ok}
.
do_create_table(set, Name, Fields) ->
  ecql:create_table(
     map_recordname(Name)
    ,[
       "a blob PRIMARY KEY, "
      ,implode(" blob, ", [map_fieldindex(I) || I <- lists:seq(1, length(Fields)-1)])
      ," blob"
     ]
    ,"ecql_mnesia_set"
  )
;
do_create_table(bag, Name, Fields) ->
  ecql:create_table(
     map_recordname(Name)
    ,[
       "a blob, "
      ,implode(" blob, ", [map_fieldindex(I) || I <- lists:seq(1, length(Fields)-1)])
      ," blob, "
      ,"PRIMARY KEY(a, "
      ,implode(", ", [map_fieldindex(I) || I <- lists:seq(1, length(Fields)-1)])
      ,")"
     ]
    ,"ecql_mnesia_bag"
  )
.

%%------------------------------------------------------------------------------
add_table_copy(_Table ,_This ,_Type) ->
  {atomic ,ok}
.


%%------------------------------------------------------------------------------
dirty_update_counter(RecordName, KeyValue, Increment) when is_atom(RecordName) ->
   case read(RecordName, KeyValue) of
    [{RecordName, KeyValue, Value}] ->
      write({RecordName, KeyValue, Value+Increment})
    ;
    [] ->
      write({RecordName, KeyValue, Increment})
    %~
   end
.

%%------------------------------------------------------------------------------
dirty_select({continuation ,[] ,_NObjects}) ->
  select({continuation ,[] ,_NObjects})
.
select({continuation ,[] ,_NObjects}) ->
  '$end_of_table'
;
select({continuation ,ResultList ,NObjects}) ->
  {Frame ,Rest} = split(NObjects ,ResultList)
 ,{Frame ,{continuation ,Rest ,NObjects}}
;
select(_Cont) ->
  '$end_of_table'
.

%%------------------------------------------------------------------------------
dirty_select(RecordName ,MatchSpec) ->
  select(RecordName ,MatchSpec)
.
select(RecordName ,MatchSpec) when is_atom(RecordName) ->
  foldr(
     fun(Record, Acc) ->
      case ets:test_ms(Record, MatchSpec) of
        {ok, false} ->
          Acc
        ;
        {ok, Result} ->
          [Result | Acc]
        ;
        _ ->
          Acc
        %~
      end
     end
    ,[]
    ,RecordName
  )
.

%%------------------------------------------------------------------------------
dirty_select(RecordName ,MatchSpec ,NObjects ,_Lock) ->
  select(RecordName ,MatchSpec ,NObjects ,_Lock)
.
select(RecordName ,MatchSpec ,NObjects ,_Lock) when is_atom(RecordName) ->
  select({continuation, select(RecordName, MatchSpec), NObjects})
.

%%------------------------------------------------------------------------------
load_textfile(Filename) ->
   {ok ,[{tables, Tables} | Records]} = file:consult(Filename)
  ,lists:foreach(
     fun({Name ,Def}) ->
       {atomic ,ok} = create_table(Name ,Def)
     end
    ,Tables
   )
  ,lists:foreach(
     fun(Record) ->
       ok = dirty_write(Record)
     end
    ,Records
   )
.

%%------------------------------------------------------------------------------
change_config(_What, NewValue) ->
  {ok, NewValue}
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Extended API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
dirty_index_match_count(_RecordName, RecordPattern, _KeyIndex) ->
  match_count(RecordPattern)
.
index_match_count(_RecordName, RecordPattern, _KeyIndex) ->
  match_count(RecordPattern)
.

%%------------------------------------------------------------------------------
dirty_index_match_count(RecordPattern, _KeyIndex) ->
  match_count(RecordPattern)
.
index_match_count(RecordPattern, _KeyIndex) ->
  match_count(RecordPattern)
.

%%------------------------------------------------------------------------------
dirty_match_count(_RecordName, RecordPattern) ->
  match_count(RecordPattern)
.
match_count(_RecordName, RecordPattern) ->
  match_count(RecordPattern)
.

%%------------------------------------------------------------------------------
dirty_match_count(RecordPattern) ->
  match_count(RecordPattern)
.
match_count(RecordPattern) when is_tuple(RecordPattern) ->
  match_count(tuple_to_list(RecordPattern))
;
match_count([RecordName | RecordValues]) when is_atom(RecordName) ->
   {Keys, Values} = and_pairs(RecordValues)
  ,ecql:select_value([
     "SELECT COUNT(*) FROM ", map_recordname(RecordName)
    ," WHERE ", Keys
    ," ALLOW FILTERING;"
   ], Values)
.

%%------------------------------------------------------------------------------
% Returns list of records
dirty_index_count(RecordName, KeyValue, KeyIndex) ->
  index_count(RecordName, KeyValue, KeyIndex)
.
index_count(RecordName, KeyValue, KeyIndex) when is_atom(RecordName) ->
  % KeyIndex is actually the list position (starting at 1) behind the RecordName
  % so we need to subtract 2 (1 for starting at one + 1 for skipping the record)
   Index = KeyIndex - 2
  ,ecql:select_value([
     "SELECT COUNT(*) FROM ", map_recordname(RecordName)
    ," WHERE ", map_fieldindex(Index), " = ?"
   ], [ecql:term_to_bin(KeyValue)])
.

%%------------------------------------------------------------------------------
dirty_count(RecordName, KeyValue) ->
  count(RecordName, KeyValue)
.
count(RecordName, KeyValue) ->
  index_count(RecordName, KeyValue, 2)
.

%%------------------------------------------------------------------------------
dirty_index_match_key(_RecordName, RecordPattern, _KeyIndex) ->
  match_key(RecordPattern)
.
index_match_key(_RecordName, RecordPattern, _KeyIndex) ->
  match_key(RecordPattern)
.

%%------------------------------------------------------------------------------
dirty_index_match_key(RecordPattern, _KeyIndex) ->
  match_key(RecordPattern)
.
index_match_key(RecordPattern, _KeyIndex) ->
  match_key(RecordPattern)
.

%%------------------------------------------------------------------------------
dirty_match_key(_RecordName, RecordPattern) ->
  match_key(RecordPattern)
.
match_key(_RecordName, RecordPattern) ->
  match_key(RecordPattern)
.

%%------------------------------------------------------------------------------
dirty_match_key(RecordPattern) ->
  match_key(RecordPattern)
.
match_key(RecordPattern) when is_tuple(RecordPattern) ->
  match_key(tuple_to_list(RecordPattern))
;
match_key([RecordName | RecordValues]) when is_atom(RecordName) ->
   {Keys, Values} = and_pairs(RecordValues)
  ,ecql:eval_all(ecql:select_column([
     "SELECT a FROM ", map_recordname(RecordName)
    ," WHERE ", Keys
    ," ALLOW FILTERING;"
   ], 1, Values))
.

%%------------------------------------------------------------------------------
% Returns list of records
dirty_index_key(RecordName, KeyValue, KeyIndex) ->
  index_key(RecordName, KeyValue, KeyIndex)
.
index_key(RecordName, KeyValue, KeyIndex) when is_atom(RecordName) ->
  % KeyIndex is actually the list position (starting at 1) behind the RecordName
  % so we need to subtract 2 (1 for starting at one + 1 for skipping the record)
   Index = KeyIndex - 2
  ,ecql:eval_all(ecql:select_column([
     "SELECT a FROM ", map_recordname(RecordName)
    ," WHERE ", map_fieldindex(Index), " = ?"
   ], 1, [ecql:term_to_bin(KeyValue)]))
.

%%------------------------------------------------------------------------------
dirty_key(RecordName, KeyValue) ->
  key(RecordName, KeyValue)
.
key(RecordName, KeyValue) ->
  index_key(RecordName, KeyValue, 2)
.

%%------------------------------------------------------------------------------
foldr(Fun, Acc, RecordName) ->
  do_foldr(Fun, next(iterator(RecordName)), Acc)
.
do_foldr(_Fun, none, Acc) ->
  Acc
;
do_foldr(Fun, {Value, Iter}, Acc) ->
  do_foldr(Fun, next(Iter), Fun(Value, Acc))
.

%%------------------------------------------------------------------------------
update_element(RecordName, Key, Tuple) when is_tuple(Tuple) ->
  update_element(RecordName, Key, [Tuple])
;
update_element(RecordName, Key, List) ->
   {RecordIndexes, RecordValues} = lists:unzip([{2, Key} | List])
  ,dirty(RecordName, Key)
  ,do_update_element(RecordName, RecordIndexes, RecordValues)
.
do_update_element(RecordName, RecordIndexes, RecordValues) ->
   FieldNames = [map_fieldindex(RecordIndex - 2) || RecordIndex <- RecordIndexes]
  ,ecql:execute(
     [
       "INSERT INTO ", map_recordname(RecordName) ," ("
      ,implode($,, FieldNames)
      ,") VALUES (?"
      ,string:copies(",?", length(RecordValues) - 1) ,");"
    ]
    ,[ecql:term_to_bin(Value) || Value <- RecordValues]
    ,?CL_LOCAL_QUORUM
  )
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Private API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

dirty(RecordName, KeyValue) ->
  case read(RecordName, KeyValue) of
    [] ->
      ok
    ;
    [Record] ->
      dirty(Record)
    %~
  end
.
dirty(Record) when is_tuple(Record) ->
   dirty(tuple_to_list(Record))
;
dirty([RecordName, KeyValue | RecordValues]) ->
   ecql_cache:dirty({RecordName, KeyValue})
  ,do_dirty(RecordName, 3, RecordValues)
.
do_dirty(_RecordName, _N, []) ->
  ok
;
do_dirty(RecordName, N, [Value | RecordValues]) ->
   ecql_cache:dirty({RecordName, Value, N})
  ,do_dirty(RecordName, N + 1, RecordValues)
.

%%------------------------------------------------------------------------------
select_records(RecordName, Cql, Args) when is_atom(RecordName) ->
   {_Keys, Rows} = ecql:select(Cql, Args)
  ,[list_to_tuple([RecordName | ecql:eval_all(RecordValues)]) || RecordValues <- Rows]
.

%%------------------------------------------------------------------------------
iterator(RecordName) when is_atom(RecordName) ->
  do_next({iterator, RecordName, [], undefined})
.

%%------------------------------------------------------------------------------
next({iterator, _RecordName, [], '$end_of_table'}) ->
  none
;
next({iterator, _RecordName, [], _LastID} = Iter) ->
  next(do_next(Iter))
;
next({iterator, RecordName, [Head | Values], LastID}) ->
  {Head, {iterator, RecordName, Values, LastID}}
.
do_next({iterator, RecordName, [], LastID}) ->
   N = 100
  ,Values = case LastID of
     undefined -> firstn(RecordName, N)
    ;_ -> nextn(RecordName, LastID, N)
   end
  ,case length(Values) of
    N ->
       LastID2 = element(2, lists:last(Values))
      ,{iterator, RecordName, Values, LastID2}
    ;
    _ ->
       {iterator, RecordName, Values, '$end_of_table'}
   end
.

%%------------------------------------------------------------------------------
firstn(RecordName, N) when is_atom(RecordName) ->
  select_records(
     RecordName
    ,["SELECT * FROM ", map_recordname(RecordName), " LIMIT ?"]
    ,[N]
  )
.

%%------------------------------------------------------------------------------
nextn(RecordName, KeyValue, N) when is_atom(RecordName) ->
  select_records(
     RecordName
    ,[
       "SELECT * FROM ", map_recordname(RecordName)
      ," WHERE token(a) > token(?) LIMIT ?"
     ]
    ,[ecql:term_to_bin(KeyValue), N]
  )
.

%%------------------------------------------------------------------------------
key_or_end([]) ->
  '$end_of_table'
;
key_or_end([Other]) ->
  element(2, Other)
.

%%------------------------------------------------------------------------------
map_fieldindex(FieldIndex) when is_integer(FieldIndex) ->
  $a + FieldIndex
.

%%------------------------------------------------------------------------------
map_recordname(RecordName) when is_atom(RecordName) ->
  [case C of $. -> $_; _ -> C end || C <- atom_to_list(RecordName)]
.

%%------------------------------------------------------------------------------
unmap_recordname(TableName) when is_binary(TableName) ->
  unmap_recordname(binary_to_list(TableName))
;
unmap_recordname(TableName) when is_list(TableName) ->
  list_to_atom([case C of $_ -> $.; _ -> C end || C <- TableName])
.

%%------------------------------------------------------------------------------
is_ref(Reference) when is_atom(Reference) ->
  case Reference of
    '$1' -> true;
    '$2' -> true;
    '$3' -> true;
    '$4' -> true;
    '$5' -> true;
    '$6' -> true;
    '$7' -> true;
    '$8' -> true;
    '$9' -> true;
    '_' -> true;
    _ -> false
  end
;
is_ref(_Reference) ->
  false
.

%%------------------------------------------------------------------------------
fields(RecordValues) ->
  [map_fieldindex(I) || I <- lists:seq(0, length(RecordValues)-1)]
.

%%------------------------------------------------------------------------------
and_pairs(List) ->
   KeyValueList = [
    {[Key, " = ?"], ecql:term_to_bin(Value)}
    || {Key, Value} <- lists:zip(fields(List), List), not is_ref(Value)
   ]
  ,{Keys, Values} = lists:unzip(KeyValueList)
  ,{implode(" AND ", Keys), Values}
.

%%------------------------------------------------------------------------------
implode(Sep, List) ->
   tl(implode_concat(Sep, List))
.

%%------------------------------------------------------------------------------
implode_concat(_Sep, []) ->
  []
;
implode_concat(Sep, [Head | Tail]) ->
  [Sep, Head] ++ implode_concat(Sep, Tail)
.

%%------------------------------------------------------------------------------
split(NObjects ,ResultList) when length(ResultList) > NObjects ->
  lists:split(NObjects ,ResultList)
;
split(_NObjects ,ResultList) ->
  {ResultList ,[]}
.

%%------------------------------------------------------------------------------
get_tables() ->
  case ecql:config(tables) of
    undefined ->
       {_, CQLTables} = ecql:select([
         "SELECT columnfamily_name, comment FROM system.schema_columnfamilies"
       ])
      ,Tables = [
        {
           unmap_recordname(TableName)
          ,[{
             type
            ,case Comment of
               "ecql_mnesia_set" -> set
              ;"ecql_mnesia_bag" -> bag
             end
           }]
        }
        || [TableName, Comment] <- CQLTables
          ,string:left(Comment, 11) =:= "ecql_mnesia"
       ]
      ,ok = ecql:config(tables, Tables)
      ,Tables
    ;
    Tables ->
      Tables
    %~
  end
.

%%==============================================================================
%% END OF FILE
