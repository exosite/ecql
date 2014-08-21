%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_decoding_1_4).
-compile(native).
-compile(inline).
-compile({inline_size,   500}).    %% default=24
-compile({inline_effort, 500}).   %% default=150
-compile({inline_unroll, 5}).

-include("../include/ecql.hrl").

%% Public API
-export([
   test/1
]).

%%------------------------------------------------------------------------------
test(X) ->
  R = lists:sum([do_test() || _ <- lists:seq(1, X)])/X,
  io:format("~p ~p iterations: ~p~n", [?MODULE, X, R])
.
do_test() ->
  {Time, _} = timer:tc(fun read_rows/2, test_data()),
  Time
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
read_rows(<<RowCount:?T_INT32, Body/binary>>, ColTypes) ->
   TLen = length(ColTypes)
  ,{Cells, Rest} = read_n_bytes(RowCount*TLen, Body)
  ,{convert_rows(ColTypes, RowCount, Cells), Rest}
.

%%------------------------------------------------------------------------------
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
test_data() ->
[<<0,0,0,9,0,0,0,46,131,109,0,0,0,40,101,56,98,101,101,99,101,54,54,101,97,102,
   52,98,57,98,54,101,100,52,97,99,100,53,54,99,50,52,56,101,51,48,102,101,48,
   56,50,53,102,55,0,0,0,6,131,98,83,244,122,140,0,0,0,15,131,109,0,0,0,9,49,
   50,55,46,48,46,48,46,49,0,0,0,44,131,109,0,0,0,38,118,101,110,100,111,114,
   61,116,101,115,116,118,101,110,100,111,114,38,109,111,100,101,108,61,116,
   101,115,116,109,111,100,101,108,38,115,110,61,49,0,0,0,46,131,109,0,0,0,40,
   101,56,98,101,101,99,101,54,54,101,97,102,52,98,57,98,54,101,100,52,97,99,
   100,53,54,99,50,52,56,101,51,48,102,101,48,56,50,53,102,55,0,0,0,6,131,98,
   83,244,122,139,0,0,0,15,131,109,0,0,0,9,49,50,55,46,48,46,48,46,49,0,0,0,44,
   131,109,0,0,0,38,118,101,110,100,111,114,61,116,101,115,116,118,101,110,100,
   111,114,38,109,111,100,101,108,61,116,101,115,116,109,111,100,101,108,38,
   115,110,61,49,0,0,0,46,131,109,0,0,0,40,101,56,98,101,101,99,101,54,54,101,
   97,102,52,98,57,98,54,101,100,52,97,99,100,53,54,99,50,52,56,101,51,48,102,
   101,48,56,50,53,102,55,0,0,0,6,131,98,83,244,122,138,0,0,0,15,131,109,0,0,0,
   9,49,50,55,46,48,46,48,46,49,0,0,0,44,131,109,0,0,0,38,118,101,110,100,111,
   114,61,116,101,115,116,118,101,110,100,111,114,38,109,111,100,101,108,61,
   116,101,115,116,109,111,100,101,108,38,115,110,61,49,0,0,0,46,131,109,0,0,0,
   40,101,56,98,101,101,99,101,54,54,101,97,102,52,98,57,98,54,101,100,52,97,
   99,100,53,54,99,50,52,56,101,51,48,102,101,48,56,50,53,102,55,0,0,0,6,131,
   98,83,244,121,40,0,0,0,15,131,109,0,0,0,9,49,50,55,46,48,46,48,46,49,0,0,0,
   44,131,109,0,0,0,38,118,101,110,100,111,114,61,116,101,115,116,118,101,110,
   100,111,114,38,109,111,100,101,108,61,116,101,115,116,109,111,100,101,108,
   38,115,110,61,49,0,0,0,46,131,109,0,0,0,40,101,56,98,101,101,99,101,54,54,
   101,97,102,52,98,57,98,54,101,100,52,97,99,100,53,54,99,50,52,56,101,51,48,
   102,101,48,56,50,53,102,55,0,0,0,6,131,98,83,244,121,39,0,0,0,15,131,109,0,
   0,0,9,49,50,55,46,48,46,48,46,49,0,0,0,44,131,109,0,0,0,38,118,101,110,100,
   111,114,61,116,101,115,116,118,101,110,100,111,114,38,109,111,100,101,108,
   61,116,101,115,116,109,111,100,101,108,38,115,110,61,49,0,0,0,46,131,109,0,
   0,0,40,101,56,98,101,101,99,101,54,54,101,97,102,52,98,57,98,54,101,100,52,
   97,99,100,53,54,99,50,52,56,101,51,48,102,101,48,56,50,53,102,55,0,0,0,6,
   131,98,83,244,121,38,0,0,0,15,131,109,0,0,0,9,49,50,55,46,48,46,48,46,49,0,
   0,0,44,131,109,0,0,0,38,118,101,110,100,111,114,61,116,101,115,116,118,101,
   110,100,111,114,38,109,111,100,101,108,61,116,101,115,116,109,111,100,101,
   108,38,115,110,61,49,0,0,0,46,131,109,0,0,0,40,101,56,98,101,101,99,101,54,
   54,101,97,102,52,98,57,98,54,101,100,52,97,99,100,53,54,99,50,52,56,101,51,
   48,102,101,48,56,50,53,102,55,0,0,0,6,131,98,83,244,121,37,0,0,0,15,131,109,
   0,0,0,9,49,50,55,46,48,46,48,46,49,0,0,0,44,131,109,0,0,0,38,118,101,110,
   100,111,114,61,116,101,115,116,118,101,110,100,111,114,38,109,111,100,101,
   108,61,116,101,115,116,109,111,100,101,108,38,115,110,61,49,0,0,0,46,131,
   109,0,0,0,40,101,56,98,101,101,99,101,54,54,101,97,102,52,98,57,98,54,101,
   100,52,97,99,100,53,54,99,50,52,56,101,51,48,102,101,48,56,50,53,102,55,0,0,
   0,6,131,98,83,244,121,36,0,0,0,15,131,109,0,0,0,9,49,50,55,46,48,46,48,46,
   49,0,0,0,44,131,109,0,0,0,38,118,101,110,100,111,114,61,116,101,115,116,118,
   101,110,100,111,114,38,109,111,100,101,108,61,116,101,115,116,109,111,100,
   101,108,38,115,110,61,49,0,0,0,46,131,109,0,0,0,40,101,56,98,101,101,99,101,
   54,54,101,97,102,52,98,57,98,54,101,100,52,97,99,100,53,54,99,50,52,56,101,
   51,48,102,101,48,56,50,53,102,55,0,0,0,6,131,98,83,244,121,35,0,0,0,15,131,
   109,0,0,0,9,49,50,55,46,48,46,48,46,49,0,0,0,44,131,109,0,0,0,38,118,101,
   110,100,111,114,61,116,101,115,116,118,101,110,100,111,114,38,109,111,100,
   101,108,61,116,101,115,116,109,111,100,101,108,38,115,110,61,49>>,
 [3,3,3,3]]
.

%%==============================================================================
%% END OF FILE

