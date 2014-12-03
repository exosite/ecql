%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_stream).
-behaviour(gen_server).
-compile(native).
-compile(inline).
-compile({inline_size,   500}).    %% default=24
-compile({inline_effort, 500}).   %% default=150
-compile({inline_unroll, 5}).

%% Public API
-export([
   foldl/6
  ,query/4
  ,query_async/4
  ,query_batch/4
  ,release/1
  ,sync/1
]).

%% OTP gen_server
-export([
   init/1
  ,start_link/3
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
  ,terminate/2
]).

%% Defines
-define(TIMEOUT, infinity).
-define(MAX_PENDING, 100).
-define(BATCH_SIZE, 1000).
-define(MAX_PENDING_BATCH, 1).
-define(DISABLED_PAGING, #paging{flag = 0}).

%% Includes
-include("ecql.hrl").

%% Records
-record(state, {
   connection, sender, stream, async_pending = 0, monitor_ref
  ,async_laststmt, async_start, laststmt, lastresult
}).
-record(metadata, {flags, columnspecs, paging_state}).
-record(preparedstatement, {cql, host, id, metadata, result_metadata}).
-record(paging, {flag = 4, page_state = <<>>, page_size = 1000}).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
foldl(Id, Fun, Acc, Cql, Args, Consistency) ->
  case prepare_statement(Id, Cql, Args) of
    {ok, Prep} ->
      do_foldl(Id, Fun, Acc, Prep, Args, Consistency)
    ;
    Error ->
      Error
    %~
  end
.
do_foldl({_, Pid} = Id, Fun, Acc, Prep, Args, Consistency) ->
   gen_server:cast(Pid, {query_start, <<>>, Prep, Args, Consistency})
  ,do_foldl_recv(Id, Fun, Acc, Prep, Args, Consistency)
.
do_foldl_recv({_, Pid} = Id, Fun, Acc, Prep, Args, Consistency) ->
  case gen_server:call(Pid, query_receive, ?TIMEOUT) of
    {<<>>, {Keys, Rows}} ->
       Fun(Keys, Rows, Acc)
    ;
    {PageState, {Keys, Rows}} when is_binary(PageState) ->
       gen_server:cast(Pid, {query_start, PageState, Prep, Args, Consistency})
      ,do_foldl_recv(Id, Fun, Fun(Keys, Rows, Acc), Prep, Args, Consistency)
    ;
    Other ->
      Other
    %~
  end
.

%%------------------------------------------------------------------------------
query(Id, Cql, Args, Consistency) ->
  case foldl(Id, fun do_query/3, {[], []}, Cql, Args, Consistency) of
    {Keys, [Rows]} ->
      {Keys, Rows}
    ;
    {Keys, Rows} when is_list(Keys) ->
      {Keys, lists:append(lists:reverse(Rows))}
    ;
    Other ->
      Other
    %~
  end
.
do_query(Keys, Rows, {_Keys, Rows0}) ->
  {Keys, [Rows | Rows0]}
.

%%------------------------------------------------------------------------------
query_async({_, Pid} = Id, Cql, Args, Consistency) ->
  case prepare_statement(Id, Cql, Args) of
    {ok, Prep} ->
      gen_server:call(Pid, {query_async, Prep, Args, Consistency}, ?TIMEOUT)
    ;
    Error ->
      Error
    %~
  end
.

%%------------------------------------------------------------------------------
query_batch(_, _, [], _) ->
  ok
;
query_batch(Id, Cql, ListOfArgs, Consistency) ->
  case prepare_statement(Id, Cql, ListOfArgs) of
    {ok, Prep} ->
      case do_query_batch(Id, Prep, ListOfArgs, Consistency) of
        ok ->
          sync(Id)
        ;
        Error ->
          Error
        %~
      end
    ;
    Error ->
      Error
    %~
  end
.
do_query_batch(Id, Prep, ListOfArgs, Consistency) when length(ListOfArgs) > ?BATCH_SIZE ->
   {ListOfArgs1, ListOfArgs2} = lists:split(?BATCH_SIZE, ListOfArgs)
  ,do_query_batch(Id, Prep, ListOfArgs1, Consistency)
  ,do_query_batch(Id, Prep, ListOfArgs2, Consistency)
;
do_query_batch({_, Pid}, Prep, ListOfArgs, Consistency) ->
   gen_server:call(Pid, {query_batch_async, Prep, ListOfArgs, Consistency}, ?TIMEOUT)
.

%%------------------------------------------------------------------------------
release({_, Pid}) ->
  gen_server:call(Pid, release, ?TIMEOUT)
.

%%------------------------------------------------------------------------------
sync({_, Pid}) ->
  gen_server:call(Pid, sync, ?TIMEOUT)
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
handle_call(query_receive, _From, State = #state{lastresult = Ret}) ->
   {reply, Ret, State#state{lastresult = undefined}}
;
handle_call({query_async, Statement, Args, Consistency}, _From, State0) ->
   State1 = #state{async_pending = Pending} = wait_async(State0, ?MAX_PENDING)
  ,execute_query(Statement, Args, Consistency, State1, ?DISABLED_PAGING)
  ,{reply, ok, State1#state{async_pending = Pending + 1, async_laststmt = Statement, async_start = now()}}
;
handle_call({prepare, Cql}, _From, State0) ->
   State1 = wait_async(State0)
  ,send_frame(
     State1
    ,?OP_PREPARE
    ,wire_longstring(Cql)
  )
  ,{reply, recv_frame(Cql), State1}
;
handle_call({query_batch, Statement, ListOfArgs, Consistency}, _From, State) ->
   State1 = wait_async(State)
  ,execute_batch(Statement, ListOfArgs, Consistency, State1)
  ,{reply, recv_frame(Statement), State1#state{async_laststmt = Statement}}
;
handle_call({query_batch_async, Cql, ListOfArgs, Consistency}, _From, State) ->
   State1 = #state{async_pending = Pending} = wait_async(State, ?MAX_PENDING_BATCH)
  ,execute_batch(Cql, ListOfArgs, Consistency, State1)
  ,{reply, ok, State1#state{async_pending = Pending + 1, async_laststmt = Cql, async_start = now()}}
;
handle_call(release, _From, State = #state{monitor_ref = undefined}) ->
   {reply, {error, already_released}, State}
;
handle_call(release, _From, State = #state{connection = Conn, monitor_ref = MonitorRef}) ->
   demonitor(MonitorRef, [flush])
  ,Conn ! {add_stream, self()}
  ,{reply, ok, State#state{monitor_ref = undefined}}
;
handle_call(sync, _From, State) ->
  {reply, ok, wait_async(State)}
.

%%------------------------------------------------------------------------------
handle_cast({query_start, PageState, Statement, Args, Consistency} ,State0) ->
   State = wait_async(State0)
  ,{Time, Ret} = timer:tc(fun() ->
     execute_query(Statement, Args, Consistency, State, #paging{page_state = PageState})
    ,recv_frame(Statement)
  end)
  ,ecql_log:log(Time, query, Statement, Args)
  ,{noreply, State#state{laststmt = Statement, lastresult = Ret}}
;
handle_cast(stop, State) ->
   wait_async(State)
  ,{stop, normal, State}
.

%%------------------------------------------------------------------------------
% Async message incoming
handle_info({frame, ResponseOpCode, ResponseBody}, State = #state{async_pending = Pending}) ->
   log(ResponseOpCode, ResponseBody, State)
  ,{noreply, State#state{async_pending = Pending - 1}}
;
handle_info({'DOWN', MonitorRef, _Type, _Object, _Info}, State = #state{connection = Conn, monitor_ref = MonitorRef}) ->
   Conn ! {add_stream, self()}
  ,{noreply, State#state{monitor_ref = undefined}}
;
handle_info({monitor, Client}, State = #state{monitor_ref = undefined}) ->
   Ref = monitor(process, Client)
  ,{noreply, State#state{monitor_ref = Ref}}
.

%%------------------------------------------------------------------------------
terminate(Reason, State) ->
  {Reason, State}
.

%%------------------------------------------------------------------------------
code_change(_ ,State ,_) ->
  {ok ,State}
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Private API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
wait_async(State) ->
  wait_async(State, 0)
.
wait_async(State = #state{async_pending = Pending}, Allowed) when Pending =< Allowed ->
  State
;
wait_async(State = #state{async_pending = Pending}, _Allowed) ->
   receive {frame, ResponseOpCode, ResponseBody} ->
     log(ResponseOpCode, ResponseBody, State)
    ,wait_async(State#state{async_pending = Pending - 1})
   end
.

%%------------------------------------------------------------------------------
log(ResponseOpCode, Body, State = #state{async_laststmt = Cql, async_start = Begin}) ->
   ecql_log:log(timer:now_diff(now(), Begin), async, Cql, [])
  ,case ResponseOpCode of
    ?OP_ERROR ->
      do_log(handle_response(?OP_ERROR, Body), State)
    ;
    _ ->
      ok
    %~
  end
.
do_log({error, Code, Message}, State) ->
  error_logger:error_msg("query_async: failed: {error, ~p, ~s} state: ~p~n", [Code, Message, State])
;
do_log(Error, State) ->
  error_logger:error_msg("query_async: failed: ~p sate: ~p~n", [Error, State])
.

%%------------------------------------------------------------------------------
% Executes a single plain query
execute_query(Cql, [], Consistency, State, Paging) ->
  Query = [
     wire_longstring(Cql)
    ,<<
       Consistency:?T_UINT16
      ,(get_flag([], Paging, undefined)):?T_UINT8
     >>
    ,get_page_size(Paging)
    ,Paging#paging.page_state
   ]
  ,send_frame(State, ?OP_QUERY, Query)
;
% Executes a prepared query
execute_query(
   #preparedstatement{
    id = Id,
    metadata = RequestMetadata,
    result_metadata = #metadata{columnspecs = ResultColspec}
  }
  ,Args
  ,Consistency
  ,State
  ,Paging
) ->
   Query = [
     <<
       (size(Id)):?T_UINT16
      ,Id/binary
      ,Consistency:?T_UINT16
      ,(get_flag(Args, Paging, ResultColspec)):?T_UINT8
     >>
    ,wire_values(Args, RequestMetadata)
    ,get_page_size(Paging)
    ,Paging#paging.page_state
   ]
  ,send_frame(State, ?OP_EXECUTE, Query)
.

%%------------------------------------------------------------------------------
execute_batch(
   #preparedstatement{id = Id, metadata = #metadata{columnspecs = {_, RequestTypes}}}
  ,ListOfArgs
  ,Consistency
  ,State
) ->
   Query = [
     <<
       1:?T_UINT8 % type == 'unlogged'
      ,(length(ListOfArgs)):?T_UINT16
     >>
    | wire_batch(Id, ListOfArgs, Consistency, RequestTypes)
   ]
  ,send_frame(State, ?OP_BATCH, Query)
.

%%------------------------------------------------------------------------------
get_page_size(#paging{flag = Flag, page_size = PageSize}) ->
  if (Flag band 4) == 4 ->
    <<PageSize:32>>
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
get_flag([], #paging{flag = Flag, page_state = PGState}, _) ->
  get_paging_flag(Flag, PGState)
;
get_flag(_Args, #paging{flag = Flag, page_state = PGState}, undefined) ->
  1 + get_paging_flag(Flag, PGState)
;
get_flag(_Args, #paging{flag = Flag, page_state = PGState}, _) ->
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
prepare_statement(_Id, Cql, []) ->
  {ok, Cql}
;
prepare_statement({Host, Pid}, Cql, _) ->
   Statement = iolist_to_binary(Cql)
  ,case ets:lookup(ecql_statements, {Host, Statement}) of
    [] ->
      case gen_server:call(Pid, {prepare, Statement}, infinity) of
        {ok, StatementRec0} ->
           StatementRec1 = StatementRec0#preparedstatement{cql = {Host, Statement}, host = Host}
          ,ets:insert(ecql_statements, StatementRec1)
          ,{ok, StatementRec1}
        ;
        Error ->
           Error
        %~
      end
    ;
    [StatementRec = #preparedstatement{}] ->
       {ok, StatementRec}
    %~
  end
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
recv_frame(Statement) ->
  receive {frame, ResponseOpCode, ResponseBody} ->
     handle_response(ResponseOpCode, ResponseBody, Statement)
  end
.

%%------------------------------------------------------------------------------
handle_response(?OP_RESULT, <<?RT_ROWS, Body/binary>>, #preparedstatement{result_metadata = #metadata{columnspecs = ColSpecs}}) when ColSpecs =/= undefined ->
   {#metadata{paging_state = PageState}, Rest} = read_metadata(Body)
  ,{PageState, rows(Rest, ColSpecs)}
;
handle_response(?OP_RESULT, <<?RT_ROWS, Body/binary>>, _) ->
   {#metadata{columnspecs = ColSpecs, paging_state = PageState}, Rest} = read_metadata(Body)
  ,{PageState, rows(Rest, ColSpecs)}
;
handle_response(OpCode, Body, _) ->
  handle_response(OpCode, Body)
.

%%------------------------------------------------------------------------------
handle_response(?OP_ERROR, <<?ER_UNPREPARED:?T_INT32, Len:?T_UINT16, Message:Len/binary, IdLen:?T_UINT16, Id:IdLen/binary>>) ->
   ets:match_delete(ecql_statements, #preparedstatement{id = Id, _ = '_'})
  ,{error, ?ER_UNPREPARED, binary_to_list(Message)}
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
readn(1, Body, Fun) ->
  {Value, Rest} = Fun(Body),
  {[Value], Rest}
;
readn(2, Body, Fun) ->
   {V1, R1} = Fun(Body)
  ,{V2, R2} = Fun(R1)
  ,{[V1, V2], R2}
;
readn(3, Body, Fun) ->
   {V1, R1} = Fun(Body)
  ,{V2, R2} = Fun(R1)
  ,{V3, R3} = Fun(R2)
  ,{[V1, V2, V3], R3}
;
readn(4, Body, Fun) ->
   {V1, R1} = Fun(Body)
  ,{V2, R2} = Fun(R1)
  ,{V3, R3} = Fun(R2)
  ,{V4, R4} = Fun(R3)
  ,{[V1, V2, V3, V4], R4}
;
readn(N, Body, Fun) ->
   {V1, R1} = Fun(Body)
  ,{V2, R2} = Fun(R1)
  ,{V3, R3} = Fun(R2)
  ,{V4, R4} = Fun(R3)
  ,{VList, R5} = readn(N-4, R4, Fun)
  ,{[V1, V2, V3, V4 | VList], R5}
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
   TLen = length(ColTypes)
  ,{Cells, Rest} = read_n_bytes(RowCount*TLen, Body)
  ,{convert_rows(ColTypes, RowCount, Cells), Rest}
.

convert_rows(_ColTypes, 0, []) ->
  []
;
convert_rows(ColTypes, 1, Cells) ->
   {Row, []} = convert_row(ColTypes, Cells)
  ,[Row]
;
convert_rows(ColTypes, 2, Cells) ->
   {Row, Rest} = convert_row(ColTypes, Cells), {Row2, []} = convert_row(ColTypes, Rest)
  ,[Row, Row2]
;
convert_rows(ColTypes, 3, Cells) ->
   {Row, Rest} = convert_row(ColTypes, Cells), {Row2, Rest2} = convert_row(ColTypes, Rest)
  ,{Row3, []} = convert_row(ColTypes, Rest2)
  ,[Row, Row2, Row3]
;
convert_rows(ColTypes, 4, Cells) ->
   {Row, Rest} = convert_row(ColTypes, Cells), {Row2, Rest2} = convert_row(ColTypes, Rest)
  ,{Row3, Rest3} = convert_row(ColTypes, Rest2), {Row4, []} = convert_row(ColTypes, Rest3)
  ,[Row, Row2, Row3, Row4]
;
convert_rows(ColTypes, N, Cells) ->
   {Row, Rest} = convert_row(ColTypes, Cells), {Row2, Rest2} = convert_row(ColTypes, Rest)
  ,{Row3, Rest3} = convert_row(ColTypes, Rest2), {Row4, Rest4} = convert_row(ColTypes, Rest3)
  ,Rows = convert_rows(ColTypes, N-4, Rest4)
  ,[Row, Row2, Row3, Row4 | Rows]
.

convert_row([], C) ->
  {[], C}
;
convert_row([T1], [C1 | C]) ->
  {[convert(T1, C1)], C}
;
convert_row([T1, T2], [C1, C2 | C]) ->
  {[convert(T1, C1), convert(T2, C2)], C}
;
convert_row([T1, T2, T3], [C1, C2, C3 | C]) ->
  {[convert(T1, C1), convert(T2, C2), convert(T3, C3)], C}
;
convert_row([T1, T2, T3, T4], [C1, C2, C3, C4 | C]) ->
  {[convert(T1, C1), convert(T2, C2), convert(T3, C3), convert(T4, C4)], C}
;
convert_row([T1, T2, T3, T4 | T], [C1, C2, C3, C4 | C]) ->
   {VList, Rest} = convert_row(T, C)
  ,{[convert(T1, C1), convert(T2, C2), convert(T3, C3), convert(T4, C4) | VList], Rest}
.

%%------------------------------------------------------------------------------
read_n_bytes(0, Body) ->
  {[], Body}
;
read_n_bytes(1, Body) ->
   {V, R} = read_bytes(Body)
  ,{[V], R}
;
read_n_bytes(2, Body) ->
   {V, R} = read_bytes(Body), {V2, R2} = read_bytes(R)
  ,{[V, V2], R2}
;
read_n_bytes(3, Body) ->
   {V, R} = read_bytes(Body), {V2, R2} = read_bytes(R)
  ,{V3, R3} = read_bytes(R2)
  ,{[V, V2, V3], R3}
;
read_n_bytes(4, Body) ->
   {V, R} = read_bytes(Body), {V2, R2} = read_bytes(R)
  ,{V3, R3} = read_bytes(R2), {V4, R4} = read_bytes(R3)
  ,{[V, V2, V3, V4], R4}
;
read_n_bytes(N, Body) ->
   {V, R} = read_bytes(Body), {V2, R2} = read_bytes(R)
  ,{V3, R3} = read_bytes(R2), {V4, R4} = read_bytes(R3)
  ,{VList, R5} = read_n_bytes(N-4, R4)
  ,{[V, V2, V3, V4 | VList], R5}
.

%%------------------------------------------------------------------------------
% [bytes]: A [int] n, followed by n bytes if n >= 0. If n < 0,
%          no byte should follow and the value represented is `null`.
read_bytes(<<Len:?T_INT32, Value:Len/binary, Rest/binary>>) ->
  {Value, Rest}
;
read_bytes(<<Len:?T_INT32, Rest/binary>>) when Len < 0 ->
  {undefined, Rest}
.

%%------------------------------------------------------------------------------
% [short bytes]  A [short] n, followed by n bytes if n >= 0.
read_sbytes(<<Len:?T_UINT16, Value:Len/binary, Rest/binary>>) ->
  {Value, Rest}
.

%%------------------------------------------------------------------------------
% The <column_name> is a [string] and <type> is an [option]
read_colspec(<<Len:?T_INT16, Name:Len/binary, Type:?T_INT16, Rest/binary>>) ->
   {TypeDef, Rest2} = read_colspec_type(Type, Rest)
  ,{{binary_to_atom(Name, utf8), TypeDef}, Rest2}
.

%%------------------------------------------------------------------------------

read_colspec_type(0, _) ->
  undefined
;
read_colspec_type(32, <<Type:?T_INT16, Rest/binary>>) ->
   {ValueType, Rest2} = read_colspec_type(Type, Rest)
  ,{{list, ValueType}, Rest2}
;
read_colspec_type(33, <<Type:?T_INT16, Rest/binary>>) ->
   {KeyType, <<Type2:?T_INT16, Rest2/binary>>} = read_colspec_type(Type, Rest)
  ,{ValueType, Rest3} = read_colspec_type(Type2, Rest2)
  ,{{map, {KeyType, ValueType}}, Rest3}
;
read_colspec_type(34, <<Type:?T_INT16, Rest/binary>>) ->
   {ValueType, Rest2} = read_colspec_type(Type, Rest)
  ,{{set, ValueType}, Rest2}
;
read_colspec_type(Type, Rest) ->
  {Type, Rest}
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
% CQL null == undefined
convert(_, undefined) ->
  undefined
;
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
convert(4, <<0>>) ->
  false
;
convert(4, <<1>>) ->
  true
;
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
;
% 0x000F    Timeuuid
% NOPE
% 0x0010    Inet
% NOPE
% 0x0020    List: the value is an [option], representing the type
%                of the elements of the list.
convert({list, ValueType}, <<Count:?T_UINT16, Body/binary>>) ->
   {Values, <<>>} = readn(Count, Body, fun read_sbytes/1)
  ,lists:map(fun(Value) -> convert(ValueType, Value) end, Values)
;
% 0x0021    Map: the value is two [option], representing the types of the
%               keys and values of the map
convert({map, {KeyType, ValueType}}, <<Count:?T_UINT16, Body/binary>>) ->
  {Values, <<>>} = readn(Count, Body, fun(BinRow) ->
     {[Key, Value], RowRest} = readn(2, BinRow, fun read_sbytes/1)
    ,{{convert(KeyType, Key), convert(ValueType, Value)}, RowRest}
  end)
  ,Values
;
% 0x0022    Set: the value is an [option], representing the type
%                of the elements of the set
convert({set, ValueType}, Binary) ->
  convert({list, ValueType}, Binary)
.

%%------------------------------------------------------------------------------
convert_int(<<Value:?T_INT64>>) ->
   Value
;
convert_int(<<Value:?T_INT32>>) ->
   Value
.

%%------------------------------------------------------------------------------
wire_values(Values, RequestMetadata) ->
   #metadata{columnspecs = {_, RequestTypes}} = RequestMetadata
  ,wire_shortlist(zipwith_wire(RequestTypes, Values))
.

%%------------------------------------------------------------------------------
zipwith_wire([T1], [V1]) ->
  [wire_longstring(wire_value(T1, V1))]
;
zipwith_wire([T1, T2], [V1, V2]) ->
  [wire_longstring(wire_value(T1, V1)), wire_longstring(wire_value(T2, V2))]
;
zipwith_wire([T1, T2, T3], [V1, V2, V3]) ->
  [
    wire_longstring(wire_value(T1, V1)), wire_longstring(wire_value(T2, V2)),
    wire_longstring(wire_value(T3, V3))
  ]
;
zipwith_wire([T1, T2, T3, T4], [V1, V2, V3, V4]) ->
  [
    wire_longstring(wire_value(T1, V1)), wire_longstring(wire_value(T2, V2)),
    wire_longstring(wire_value(T3, V3)), wire_longstring(wire_value(T4, V4))
  ]
;
zipwith_wire([T1, T2, T3, T4 | T], [V1, V2, V3, V4 | V]) ->
  [
    wire_longstring(wire_value(T1, V1)), wire_longstring(wire_value(T2, V2)),
    wire_longstring(wire_value(T3, V3)), wire_longstring(wire_value(T4, V4))
    |zipwith_wire(T, V)
  ]
.

%%------------------------------------------------------------------------------
wire_batch(Id, ListOfArgs, Consistency, RequestTypes) ->
  Head = <<
     1:?T_UINT8 % kind == 'prepared query'
    ,(size(Id)):?T_UINT16
    ,Id/binary
    ,(length(RequestTypes)):?T_UINT16
  >>
  ,do_wire_batch(Head, ListOfArgs, Consistency, RequestTypes)
.
do_wire_batch(Head, [Args | ListOfArgs], Consistency, RequestTypes) ->
  [
     Head
    ,zipwith_wire(RequestTypes, Args)
    |do_wire_batch(Head, ListOfArgs, Consistency, RequestTypes)
  ]
;
do_wire_batch(_Head, [], Consistency, _RequestTypes) ->
  [<<
    Consistency:?T_UINT16
  >>]
.

%%------------------------------------------------------------------------------
% 0x0001    Ascii
wire_value(1, Value) ->
  Value
;
% 0x0002    Bigint
wire_value(2, Value) ->
  wire_bigint(Value)
;
% 0x0003    Blob
wire_value(3, Value) ->
  Value
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
  Value
;
% 0x000E    Varint
wire_value(14, Value) ->
  wire_bigint(Value)
;
% 0x000F    Timeuuid
% NOPE
% 0x0010    Inet
% NOPE
% 0x0020    List: the value is an [option], representing the type
%                of the elements of the list.
wire_value({list, ValueType}, Values) when is_list(Values) ->
  wire_shortlist(lists:map(fun(Value) ->
    wire_shortstring(wire_value(ValueType, Value))
  end, Values))
;
wire_value({list, ValueType}, Value) ->
  wire_value({list, ValueType}, [Value])
;
% 0x0021    Map: the value is two [option], representing the types of the
%               keys and values of the map
wire_value({map, {KeyType, ValueType}}, Values) when is_list(Values) ->
  wire_shortlist(lists:map(fun({Key, Value}) -> [
     wire_shortstring(wire_value(KeyType, Key))
    ,wire_shortstring(wire_value(ValueType, Value))
  ] end, Values))
;
wire_value({map, ValueType}, Value) when is_tuple(Value) ->
  wire_value({map, ValueType}, [Value])
;
% 0x0022    Set: the value is an [option], representing the type
%                of the elements of the set
wire_value({set, ValueType}, Value) ->
  wire_value({list, ValueType}, Value)
.

%%------------------------------------------------------------------------------
wire_bigint(Value) ->
  <<Value:?T_INT64>>
.

%%------------------------------------------------------------------------------
wire_int(Value) when Value > 2147483647 ->
   error_logger:error_msg("wire_int(): truncating integer ~p~n", [Value])
  ,wire_int(2147483647)
;
wire_int(Value) ->
  <<Value:?T_INT32>>
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

%%------------------------------------------------------------------------------
wire_shortlist(Value) ->
  [<<(length(Value)):?T_UINT16>>, Value]
.

%%------------------------------------------------------------------------------
wire_shortstring(Value) ->
  [<<(iolist_size(Value)):?T_UINT16>>, Value]
.

%%==============================================================================
%% END OF FILE
