%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_stream.erl - Handler for one ongoing ecql stream
%%==============================================================================
-module(ecql_decoding).

-include("../include/ecql.hrl").

%% Public API
-export([
   test/1
]).

%%------------------------------------------------------------------------------
test(X) ->
  R = lists:sum([do_test() || _ <- lists:seq(1, X)])/X,
  io:format("~p iterations: ~p~n", [X, R])
.
do_test() ->
  {Time, _} = timer:tc(fun read_rows/2, test_data()),
  Time
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
read_rows(<<RowCount:?T_INT32, Body/binary>>, ColTypes) ->
  readn(RowCount, Body, fun(BinRow) ->
     {Row, RowRest} = readn(length(ColTypes), BinRow, fun read_bytes/1)
    ,{lists:zipwith(fun convert/2, ColTypes, Row), RowRest}
  end)
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

