%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_connection.erl - Connection keeper
%%==============================================================================
-module(ecql_connection).
-behaviour(gen_server).

%% Public API
-export([
   get_stream/1
  ,get_streams/1
]).

%% OTP gen_server
-export([
   init/1
  ,start_link/1
  ,stop/1
  ,handle_call/3
  ,handle_cast/2
  ,handle_info/2
  ,code_change/3
  ,terminate/2
]).

%% Includes
-include("ecql.hrl").

%% Records
-record(state, {socket, pool, counter, sender}).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
get_stream(Connection) ->
  gen_server:call(Connection, get_stream, infinity)
.

%%------------------------------------------------------------------------------
get_streams(Connection) ->
  tuple_to_list(gen_server:call(Connection, get_streams, infinity))
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link(Configuration) ->
  gen_server:start_link(?MODULE, Configuration ,[])
.

%%------------------------------------------------------------------------------
init(Configuration) ->
   Buff = 1024*1024
  ,Opts = [{active, true}, {mode, binary}, {recbuf, Buff}, {sndbuf, Buff}]
  ,Host = proplists:get_value(host, Configuration, {127,0,0,1})
  ,Port = proplists:get_value(port, Configuration, 9042)
  ,{ok, Socket} = gen_tcp:connect(Host, Port, Opts, 2000)
  ,ok = gen_tcp:send(Socket, frame_startup())
  ,User = proplists:get_value(user, Configuration, "")
  ,Pass = proplists:get_value(pass, Configuration, "")
  ,ok = auth(waitforframe(), User, Pass)
  ,PoolSize = proplists:get_value(streams_per_connection, Configuration, 25)
  ,{ok, Sender} = ecql_sender:start_link(Socket)
  ,Pool = list_to_tuple(init_pool(PoolSize, Sender))
  ,{ok, #state{socket=Socket, pool=Pool, counter=0, sender = Sender}}
.
init_pool(0, _Sender) ->
  []
;
init_pool(N, Sender) ->
  init_pool(N-1, Sender) ++ [init_stream(N, Sender)]
.
init_stream(N, Sender) ->
   {ok, Pid} = ecql_stream:start_link(self(), Sender, N-1)
  ,Pid
.

%%------------------------------------------------------------------------------
stop(Connection) ->
  gen_server:call(Connection, stop)
.

%%------------------------------------------------------------------------------
handle_call(get_stream, _From, State = #state{pool=Pool, counter=Counter}) ->
   StreamId = (Counter rem size(Pool)) + 1
  ,{reply, element(StreamId, Pool), State#state{counter=Counter+1}}
;
handle_call(get_streams, _From, State = #state{pool=Pool}) ->
  {reply, Pool, State}
;
handle_call(stop, _From, State = #state{socket=Socket, pool=Pool}) ->
   gen_tcp:close(Socket)
  ,[ecql_stream:stop(Stream) || Stream <- tuple_to_list(Pool)]
  ,{stop, normal, ok, State}
.

%%------------------------------------------------------------------------------
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
.

%%------------------------------------------------------------------------------
handle_info({tcp, _Socket, Data}, State = #state{pool = Pool}) ->
   handle_data(Data, Pool)
  ,{noreply, State}
;
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
%% Private API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
handle_data(<<>>, _) ->
  ok
;
handle_data(Sofar, Pool) ->
   {#frame{opcode = OpCode, body = Body, stream = StreamId}, Rest} = waitforframe(Sofar)
  ,element(StreamId + 1, Pool) ! {frame, OpCode, Body}
  ,handle_data(Rest, Pool)
.

%%------------------------------------------------------------------------------
auth({#frame{opcode=?OP_READY}, _}, _User, _Pass) ->
  ok
.

%%------------------------------------------------------------------------------
waitforframe() ->
  waitforframe(<<>>)
.
waitforframe(<<?VS_RESPONSE, 0, StreamId, OpCode, Length:?T_UINT32, FrameBody:Length/binary, Rest/binary>>) ->
  {#frame{stream=StreamId, opcode=OpCode, body=FrameBody}, Rest}
;
waitforframe(<<?VS_RESPONSE, 0, StreamId, OpCode, Length:?T_UINT32, PartialFrameBody/binary>>) ->
   <<FrameBody:Length/binary, Rest/binary>> = waitforframe(Length, [PartialFrameBody])
  ,{#frame{stream=StreamId, opcode=OpCode, body=FrameBody}, Rest}
;
waitforframe(IncompleteHeader) ->
  receive {tcp, _Socket, Data} ->
    waitforframe(<<IncompleteHeader/binary, Data/binary>>)
  end
.
waitforframe(Length, PartialFrameBody) ->
  case (iolist_size(PartialFrameBody) < Length) of
    true ->
      receive {tcp, _Socket, Data} ->
        waitforframe(Length, [Data | PartialFrameBody])
      end
    ;
    false ->
      iolist_to_binary(lists:reverse(PartialFrameBody))
    %~
  end
.

%%------------------------------------------------------------------------------
frame(StreamId, OpCode, Body) ->
  [
     ?VS_REQUEST
    ,0
    ,StreamId
    ,OpCode
    ,<<(iolist_size(Body)):?T_UINT32>>
    ,Body
  ]
.
%%------------------------------------------------------------------------------
frame_startup() ->
  frame(0, ?OP_STARTUP, wire_map(["CQL_VERSION", "3.0.0"]))
.

%%------------------------------------------------------------------------------
wire_map(Strings) ->
   Length = round(length(Strings)/2)
  ,[<<Length:?T_UINT16>> | lists:map(fun wire_string/1, Strings)]
.

%%------------------------------------------------------------------------------
wire_string(String) ->
  [<<(iolist_size(String)):?T_UINT16>>, String]
.

%%==============================================================================
%% END OF FILE
