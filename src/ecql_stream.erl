%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_stream).
-behaviour(gen_server).

%% Public API
-export([
   query/2
  ,query/4
  ,query_async/4
  ,query_batch/4
]).

%% OTP gen_server
-export([
   init/1
  ,start_link/3
  ,stop/1
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
  ,terminate/2
]).

%% Defines
-define(TIMEOUT, infinity).
-define(MAX_PENDING, 100).
-define(ENABLED_PAGING, #paging{flag = 4, page_state = <<>>}).
-define(DISABLED_PAGING, #paging{flag = 0, page_state = <<>>}).
-define(RESULT_PAGE_SIZE, 1000).

%% Includes
-include("ecql.hrl").

%% Records
-record(state, {connection, sender, stream, async_pending = 0}).
-record(metadata, {flags, columnspecs, paging_state}).
-record(preparedstatement, {id, metadata, result_metadata}).
-record(paging, {flag, page_state}).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
query(Id, Cql) ->
  query(Id, Cql, [], ?CL_ONE)
.
query(Id, Cql, Args, Consistency) ->
  gen_server:call(Id, {query, Cql, Args, Consistency}, ?TIMEOUT)
.

%%------------------------------------------------------------------------------
query_async(Id, Cql, Args, Consistency) ->
   case tick(?MAX_PENDING) of
      true ->
        gen_server:call(Id, {query, Cql, Args, Consistency}, ?TIMEOUT)
      ;
      false ->
        gen_server:cast(Id, {query, Cql, Args, Consistency})
      %~
   end
  ,ok
.

%%------------------------------------------------------------------------------
query_batch(_, _, [], _) ->
  ok
;
query_batch(Id, Cql, ListOfArgs, Consistency) when length(ListOfArgs) > 65535 ->
   {ListOfArgs1, ListOfArgs2} = lists:split(65535, ListOfArgs)
  ,query_batch(Id, Cql, ListOfArgs1, Consistency)
  ,query_batch(Id, Cql, ListOfArgs2, Consistency)
;
query_batch(Id, Cql, ListOfArgs, Consistency) ->
  gen_server:call(Id, {query_batch, Cql, ListOfArgs, Consistency}, ?TIMEOUT)
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link(Connection, Sender, StreamId) ->
  gen_server:start_link(?MODULE, {Connection, Sender, StreamId} ,[])
.

%%------------------------------------------------------------------------------
init({Connection, Sender, StreamId}) ->
  {ok, #state{connection = Connection, sender = Sender, stream = StreamId}}
.

%%------------------------------------------------------------------------------
stop(Stream) ->
  gen_server:call(Stream, stop, ?TIMEOUT)
.

%%------------------------------------------------------------------------------
handle_call({query, Cql, [], Consistency}, _From, State) ->
   State1 = wait_async(State)
  ,{ok, Result} = do_query(Cql, [], Consistency, State1, ?ENABLED_PAGING, [])
  ,{reply, Result, State1}
;
handle_call({query, Cql, Args, Consistency}, _From, State) ->
   State1 = wait_async(State)
  ,{ok, Result, State2} = do_query(Cql, Args, Consistency, State1, ?ENABLED_PAGING, [])
  ,{reply, Result, State2}
;
handle_call({query_batch, Cql, ListOfArgs, Consistency}, _From, State) ->
   State1 = wait_async(State)
  ,{ok, State2} = execute_batch(Cql, ListOfArgs, Consistency, State1)
  ,{reply, recv_frame(), State2}
;
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}
.

%%------------------------------------------------------------------------------
handle_cast({query, Cql, [], Consistency}, State = #state{async_pending = Pending}) ->
   execute_query(Cql, [], Consistency, State, ?DISABLED_PAGING)
  ,{noreply, State#state{async_pending = Pending + 1}}
;
handle_cast({query, Cql, Args, Consistency}, State) ->
   {ok, _, State1 = #state{async_pending = Pending}} = execute_query(Cql, Args, Consistency, State, ?DISABLED_PAGING)
  ,{noreply, State1#state{async_pending = Pending + 1}}
;
handle_cast({query_batch, Cql, ListOfArgs, Consistency}, State) ->
   {ok, State1 = #state{async_pending = Pending}} = execute_batch(Cql, ListOfArgs, Consistency, State)
  ,{noreply, State1#state{async_pending = Pending + 1}}
;
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
.

%%------------------------------------------------------------------------------
% Async message incoming
handle_info({frame, ResponseOpCode, ResponseBody}, State = #state{async_pending = Pending}) ->
   log(ResponseOpCode, ResponseBody)
  ,{noreply, State#state{async_pending = Pending - 1}}
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
wait_async(State = #state{async_pending = 0}) ->
  State
;
wait_async(State = #state{async_pending = Pending}) ->
   receive {frame, ResponseOpCode, ResponseBody} ->
     log(ResponseOpCode, ResponseBody)
    ,wait_async(State#state{async_pending = Pending - 1})
   end
.

%%------------------------------------------------------------------------------
log(?OP_ERROR, Body) ->
  do_log(handle_response(?OP_ERROR, Body))
;
log(_ResponseOpCode, _ResponseBody) ->
  ok
.
do_log({error, ?ER_UNPREPARED, _Message}) ->
  %% Error already logged from handle_response.
  ok
;
do_log({error, Code, Message}) ->
  error_logger:error_msg("query_async: failed: {error, ~p, ~s}~n", [Code, Message])
;
do_log(Error) ->
  error_logger:error_msg("query_async: failed: ~p~n", [Error])
.

%%------------------------------------------------------------------------------
%%    do_query(Cql, [], Quorum, State, Paging, Rows) ->
%%      {ok, Result} | {ok, Result, State2}
%%
%%  Description:
%%    Send one or multiple queries to Cassandra to retrieve the Result of the
%%    given Cql.
%%------------------------------------------------------------------------------
do_query(Cql, [], Consistency, State, Paging1, Rows) ->
   execute_query(Cql, [], Consistency, State, Paging1)
  ,RecvResult = recv_frame()
  ,case redo_query(RecvResult, Paging1, Rows) of
    {no, Result} ->
      {ok, Result}
    ;
    {yes, Paging2, RecvR} ->
      do_query(Cql, [], Consistency, State, Paging2, Rows ++ RecvR)
  end
;
do_query(Cql, Args, Consistency, State1, Paging1, Rows) ->
   {ok, Metadata, State2} = execute_query(Cql, Args, Consistency, State1, Paging1)
  ,RecvResult = recv_frame(Metadata)
  ,case redo_query(RecvResult, Paging1, Rows) of
    {no, Result} ->
      {ok, Result, State2}
    ;
    {yes, Paging2, RecvR} ->
      do_query(Cql, Args, Consistency, State1, Paging2, Rows ++ RecvR)
  end
.

%%------------------------------------------------------------------------------
%%  Function:
%%    redo_query(Result) -> {no, Result} | yes
%%
%%  Description:
%%    According to the given Result, check if it is necessary to send another
%%    query, i.e., we received paging_state from Cassandra where it would be
%%    used in the next query.
%%------------------------------------------------------------------------------
redo_query(ok, _, _) ->
  {no, ok}
;
redo_query(Result, Paging1, AccRows) ->
  case erlang:element(1, Result) of
    <<>> ->
       {<<>>, {Keys, Rows}} = Result
      ,{no, {Keys, AccRows ++ Rows}}
    ;
    ok ->
      {no, Result}
    ;
    error ->
      {no, Result}
    ;
    _ ->
       {RecvPGS, {_RecvK, RecvR}} = Result
      ,Paging2 = Paging1#paging{page_state = RecvPGS}
      ,{yes, Paging2, RecvR}
  end
.

%%------------------------------------------------------------------------------
% Executes a single plain query
execute_query(Cql, [], Consistency, State, Paging) ->
  Query = [
     wire_longstring(Cql)
    ,<<
       Consistency:?T_UINT16
      ,(get_flag([], Paging)):?T_UINT8
     >>
    ,get_page_size(Paging)
    ,Paging#paging.page_state
   ]
  ,ok = send_frame(State, ?OP_QUERY, Query)
  ,ok
;
% Lazily prepares a query that has args and then executes it
execute_query(Cql, Args, Consistency, State, Paging) ->
   {StatementRec, State1} = prepare_statement(Cql, State)
  ,#preparedstatement{id = Id, result_metadata = Metadata, metadata = RequestMetadata} = StatementRec
  ,Query = [
     <<
       (size(Id)):?T_UINT16
      ,Id/binary
      ,Consistency:?T_UINT16
      ,(get_flag(Args, Paging)):?T_UINT8
     >>
    ,wire_values(Args, RequestMetadata)
    ,get_page_size(Paging)
    ,Paging#paging.page_state
   ]
  ,ok = send_frame(State1, ?OP_EXECUTE, Query)
  ,{ok, Metadata, State1}
.

%%------------------------------------------------------------------------------
execute_batch(Cql, ListOfArgs, Consistency, State) ->
   {StatementRec, State1} = prepare_statement(Cql, State)
  ,#preparedstatement{id = Id, metadata = RequestMetadata} = StatementRec
  ,Query = [
     <<
       1:?T_UINT8 % type == 'unlogged'
      ,(length(ListOfArgs)):?T_UINT16
     >>
    | wire_batch(Id, ListOfArgs, Consistency, RequestMetadata)
   ]
  ,ok = send_frame(State1, ?OP_BATCH, Query)
  ,{ok, State1}
.
wire_batch(_Id, [], Consistency, _RequestMetadata) ->
  [<<
    Consistency:?T_UINT16
  >>]
;
wire_batch(Id, [Args | ListOfArgs], Consistency, RequestMetadata) ->
  [
     <<
       1:?T_UINT8 % kind == 'prepared query'
      ,(size(Id)):?T_UINT16
      ,Id/binary
     >>
    ,wire_values(Args, RequestMetadata)
    | wire_batch(Id, ListOfArgs, Consistency, RequestMetadata)
  ]
.

get_page_size(#paging{flag = Flag}) ->
  if (Flag band 4) == 4 ->
    <<?RESULT_PAGE_SIZE:32>>
  ;
  true ->
    <<>>
  end
.

%%------------------------------------------------------------------------------
%%  Function:
%%    get_flag(Args, Paging) -> flag
%%
%%  Description:
%%    Evaluate what value the flag would be
%%
%%    1: Values, 2: skip_metadata, 4: page_size, 8: with_paging_state,
%%    10: serial_consistency
%%------------------------------------------------------------------------------
get_flag([], #paging{flag = Flag, page_state = PGState}) ->
  get_paging_flag(Flag, PGState)
;
get_flag(_Args, #paging{flag = Flag, page_state = PGState}) ->
  (3 + get_paging_flag(Flag, PGState))
.

%%------------------------------------------------------------------------------
%%  Function:
%%    get_paging_flag(Flag, PGState) -> paging flag number
%%
%%  Description:
%%    Evalute the paging flag number.
%%------------------------------------------------------------------------------
get_paging_flag(Flag, PGState) ->
   CheckFlag = Flag band 4
  ,case {CheckFlag, PGState} of
    {0, <<>>} ->
      0
    ;
    {0, _S} ->
      8
    ;
    {4, <<>>} ->
      4
    ;
    {4, _S} ->
      12
  end
.

%%------------------------------------------------------------------------------
prepare_statement(Cql, State) ->
   Statement = iolist_to_binary(Cql)
  ,case ets:lookup(ecql_statements, Statement) of
    [] ->
       State1 = wait_async(State)
      ,StatementRec = prepare_query(Cql, State1)
      ,Id = StatementRec#preparedstatement.id
      ,ets:insert(ecql_statements, {Statement, StatementRec ,Id})
      ,{StatementRec, State1}
    ;
    [{Statement, StatementRec ,_Id}] ->
       {StatementRec, State}
    %~
   end
.

%%------------------------------------------------------------------------------
prepare_query(Statement, State) ->
   ok = send_frame(
     State
    ,?OP_PREPARE
    ,wire_longstring(Statement))
  ,{ok, Id} = recv_frame()
  ,Id
.

%%------------------------------------------------------------------------------
send_frame(
   #state{sender=Sender, stream=StreamId}
  ,OpCode
  ,Body
) ->
   Frame =   [
     ?VS_REQUEST
    ,0
    ,StreamId
    ,OpCode
    ,<<(iolist_size(Body)):?T_UINT32>>
    ,Body
  ]
  % 1) Sending to the 'Sender' allows batching gen_tcp:send's together
  % 2) Sending it directly to the gen_tcp:send's might distribute better
  % Based on testing 'Sender' approach performs 20% better on multithreaded loads
  ,Sender ! {send, Frame}
  ,ok
.

%%------------------------------------------------------------------------------
recv_frame() ->
  recv_frame(undefined)
.
recv_frame(Metadata) ->
  receive {frame, ResponseOpCode, ResponseBody} ->
     handle_response(ResponseOpCode, ResponseBody, Metadata)
  end
.

%%------------------------------------------------------------------------------
handle_response(?OP_RESULT, <<?RT_ROWS, Body/binary>>, undefined) ->
   {#metadata{columnspecs = ColSpecs, paging_state = PageState}, Rest} = read_metadata(Body)
  ,{PageState, rows(Rest, ColSpecs)}
;
handle_response(?OP_RESULT, <<?RT_ROWS, Body/binary>>, #metadata{columnspecs = ColSpecs}) ->
   {#metadata{paging_state = PageState}, Rest} = read_metadata(Body)
  ,{PageState, rows(Rest, ColSpecs)}
;
handle_response(OpCode, Body, _) ->
  handle_response(OpCode, Body)
.

%%------------------------------------------------------------------------------
handle_response(?OP_ERROR, <<?ER_UNPREPARED:?T_INT32, Len:?T_UINT16, Message:Len/binary, IdLen:?T_UINT16, Id:IdLen/binary>>) ->
   ets:match_delete(ecql_statements ,{'_', '_', Id})
  ,SMessage = binary_to_list(Message)
  ,SId = binary_to_list(Id)
  ,error_logger:error_msg("query failed: {error, ~p, ~s ,~s}~n", [?ER_UNPREPARED, Message ,SId])
  ,{error, ?ER_UNPREPARED, SMessage}
;
handle_response(?OP_ERROR, <<Code:?T_INT32, Len:?T_UINT16, Message:Len/binary, _Rest/binary>>) ->
  {error, Code, binary_to_list(Message)}
;
handle_response(?OP_RESULT, <<?RT_VOID, _/binary>>) ->
  ok
;
handle_response(?OP_RESULT, <<?RT_SETKEYSPACE, _/binary>>) ->
  ok
;
handle_response(?OP_RESULT, <<?RT_PREPARED, Len:?T_UINT16, Id:Len/binary, Body/binary>>) ->
   {Metadata, Rest} = read_metadata(Body)
  ,{ResultMetadata, <<>>} = read_metadata(Rest)
  ,{ok, #preparedstatement{id = Id, metadata = Metadata, result_metadata = ResultMetadata}}
;
handle_response(?OP_RESULT, <<?RT_SCHEMACHANGE, _/binary>>) ->
  ok
;
handle_response(OpCode, Binary) ->
  {error, -1, {notyetimplemented, OpCode, Binary}}
.

%%------------------------------------------------------------------------------
readn(0, Body, _Fun) ->
  {[], Body}
;
readn(N, Body, Fun) ->
   {Value, Rest0} = Fun(Body)
  ,{Values, Rest1} = readn(N-1, Rest0, Fun)
  ,{[Value | Values], Rest1}
.

%%------------------------------------------------------------------------------
% <global_table_spec> is present if the Global_tables_spec is set in
% <flags>. If present, it is composed of two [string] representing the
% (unique) keyspace name and table name the columns return are of.
read_tablespec(Body) ->
   <<
     KLen:?T_UINT16, KeySpace:KLen/binary
    ,TLen:?T_UINT16, TableName:TLen/binary
    ,Rest/binary
   >> = Body
  ,{{KeySpace, TableName}, Rest}
.

%%------------------------------------------------------------------------------
rows(Body, {Keys, Types}) ->
   {Rows, <<>>} = read_rows(Body, Types)
  ,{Keys, Rows}
.

%%------------------------------------------------------------------------------
read_rows(<<RowCount:?T_INT32, Body/binary>>, ColTypes) ->
  readn(RowCount, Body, fun(BinRow) ->
     {Row, RowRest} = readn(length(ColTypes), BinRow, fun read_bytes/1)
    ,{lists:zipwith(fun convert/2, ColTypes, Row), RowRest}
  end)
.

%%------------------------------------------------------------------------------
% [bytes]: A [int] n, followed by n bytes if n >= 0. If n < 0,
%          no byte should follow and the value represented is `null`.
read_bytes(<<0:?T_INT32, Rest/binary>>) ->
  {<<>>, Rest}
;
read_bytes(<<Len:?T_INT32, Value:Len/binary, Rest/binary>>) ->
  {Value, Rest}
.

%%------------------------------------------------------------------------------
% The <column_name> is a [string] and <type> is an [option]
read_colspec(<<Len:?T_INT16, Name:Len/binary, Type:?T_INT16, Rest/binary>>) when Type =/= 0 ->
  {{binary_to_atom(Name, utf8), Type}, Rest}
.

%%------------------------------------------------------------------------------
read_metadata(<<Flag0:?T_INT32, ColCount:?T_INT32, Body0/binary>>) ->
   {Flag1, PageState, Body1} = retrieve_pagestate(<<Flag0:32>>, Body0)
  ,read_metadata(Flag1, PageState, ColCount, Body1)
.
% table spec per column
read_metadata(<<0:?T_INT32>>, PageState, ColCount, Body) ->
   {ColSpecs, Rest} = readn(ColCount, Body, fun(ColSpecBin) ->
     {_TableSpec, ColSpecBinRest0} = read_tablespec(ColSpecBin)
    ,read_colspec(ColSpecBinRest0)
   end)
  ,{#metadata{flags = 0, columnspecs = format_specs(ColSpecs)
    ,paging_state = PageState}, Rest}
;
% global table spec only once
read_metadata(<<1:?T_INT32>>, PageState, ColCount, Body) ->
   {_TableSpec, Rest0} = read_tablespec(Body)
  ,{ColSpecs, Rest1} = readn(ColCount, Rest0, fun read_colspec/1)
  ,{#metadata{flags = 1, columnspecs = format_specs(ColSpecs)
    ,paging_state = PageState}, Rest1}
;
% no metadata
read_metadata(<<4:?T_INT32>>, PageState, _ColCount, Body) ->
  {#metadata{flags = 4, paging_state = PageState}, Body}
;
% no metadata + global table spec is actually the same
read_metadata(<<5:?T_INT32>>, PageState, _ColCount, Body) ->
  {#metadata{flags = 5, paging_state = PageState}, Body}
.

%%------------------------------------------------------------------------------
%%  Function:
%%    retrieve_pagestate(Flag, Body) -> {NewFlag, PageState, NewBody}
%%
%%  Description:
%%    Check if the flag of Has_more_page is set.  If it is set, PageState will
%%    include the value of paging_state else PageState is <<>>.
%%    If Has_more_page is set, Flag and Body will also be updated (remove
%%    Has_more_page from Flag and remove paging_state from Body.)
%%------------------------------------------------------------------------------
retrieve_pagestate(<<Front:30, 1:1, Global:1>>, Body0) ->
   {PageState, Body1} = retrieve_pagestate(Body0)
  ,{<<Front:30, 0:1, Global:1>>, PageState, Body1}
;
retrieve_pagestate(<<Front:30, 0:1, Global:1>>, Body) ->
  {<<Front:30, 0:1, Global:1>>, <<>>, Body}
.

retrieve_pagestate(<<N:?T_INT32, PageState:N/binary, Body/binary>>) ->
  {<<N:?T_INT32, PageState/binary>>, Body}
.

%%------------------------------------------------------------------------------
format_specs(ColSpecs) ->
  lists:unzip(ColSpecs)
.

%%------------------------------------------------------------------------------
% 0x0001    Ascii
convert(1, Value) ->
  binary_to_list(Value)
;
% 0x0002    Bigint
convert(2, Value) ->
  convert_int(Value)
;
% 0x0003    Blob
convert(3, Value) ->
   Value
;
% 0x0004    Boolean
% NOPE
% 0x0005    Counter
convert(5, Value) ->
  convert_int(Value)
;
% 0x0006    Decimal
% NOPE
% 0x0007    Double
% NOPE
% 0x0008    Float
% NOPE
% 0x0009    Int
convert(9, Value) ->
  convert_int(Value)
;
% 0x000B    Timestamp
convert(11, Value) ->
  convert_int(Value)
;
% 0x000C    Uuid
convert(12, Value) ->
  Value
;
% NOPE
% 0x000D    Varchar
convert(13, Value) ->
  binary_to_list(Value)
;
% 0x000E    Varint
convert(14, Value) ->
  convert_int(Value)
.
% 0x000F    Timeuuid
% NOPE
% 0x0010    Inet
% NOPE
% 0x0020    List: the value is an [option], representing the type
%                of the elements of the list.
% NOPE
% 0x0021    Map: the value is two [option], representing the types of the
%               keys and values of the map
% NOPE
% 0x0022    Set: the value is an [option], representing the type
%                of the elements of the set
% NOPE

%%------------------------------------------------------------------------------
convert_int(<<Value:?T_INT64>>) ->
   Value
;
convert_int(<<Value:?T_INT32>>) ->
   Value
.

%%------------------------------------------------------------------------------
tick(Max) ->
   N = case get(ecql_tick) of
    undefined ->
      0;
    X ->
      X
    %~
   end
  ,case N >= Max of
    true ->
       put(ecql_tick, 0)
      ,true
    ;
    false ->
       put(ecql_tick, N + 1)
      ,false
    %~
   end
.

%%------------------------------------------------------------------------------
wire_values(Values, RequestMetadata) ->
   #metadata{columnspecs = {_, RequestTypes}} = RequestMetadata
  ,Length = length(Values)
  ,[<<Length:?T_UINT16>> | zipwith_wire(RequestTypes, Values, [])]
.

%%------------------------------------------------------------------------------
zipwith_wire([], [], Acc) ->
  lists:reverse(Acc)
;
zipwith_wire([T | RequestTypes], [V | Values], Acc) ->
  zipwith_wire(RequestTypes, Values, [wire_value(T, V) | Acc])
.


%%------------------------------------------------------------------------------
% 0x0001    Ascii
wire_value(1, Value) ->
  wire_longstring(Value)
;
% 0x0002    Bigint
wire_value(2, Value) ->
  wire_bigint(Value)
;
% 0x0003    Blob
wire_value(3, Value) ->
  wire_longstring(Value)
;
% 0x0004    Boolean
% NOPE
% 0x0005    Counter
wire_value(5, Value) ->
  wire_bigint(Value)
;
% 0x0006    Decimal
% NOPE
% 0x0007    Double
% NOPE
% 0x0008    Float
% NOPE
% 0x0009    Int
wire_value(9, Value) ->
  wire_int(Value)
;
% 0x000B    Timestamp
wire_value(11, Value) ->
  wire_int(Value)
;
% NOPE
% 0x000C    Uuid
% NOPE
% 0x000D    Varchar
wire_value(13, Value) ->
  wire_longstring(Value)
;
% 0x000E    Varint
wire_value(14, Value) ->
  wire_bigint(Value)
.
% 0x000F    Timeuuid
% NOPE
% 0x0010    Inet
% NOPE
% 0x0020    List: the value is an [option], representing the type
%                of the elements of the list.
% NOPE
% 0x0021    Map: the value is two [option], representing the types of the
%               keys and values of the map
% NOPE
% 0x0022    Set: the value is an [option], representing the type
%                of the elements of the set
% NOPE

%%------------------------------------------------------------------------------
wire_bigint(Value) ->
  <<8:?T_INT32, Value:?T_INT64>>
.

%%------------------------------------------------------------------------------
wire_int(Value) ->
  <<4:?T_INT32, Value:?T_INT32>>
.

%%------------------------------------------------------------------------------
wire_longstring(Value) when is_binary(Value) ->
  <<(size(Value)):?T_INT32, Value/binary>>
;
wire_longstring(Value) when is_list(Value) ->
  [<<(iolist_size(Value)):?T_INT32>>, Value]
;
wire_longstring(Value) when is_atom(Value) ->
  wire_longstring(atom_to_binary(Value, utf8))
.

%%==============================================================================
%% END OF FILE
