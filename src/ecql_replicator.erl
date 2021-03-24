%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_replicator.erl - Replicator that forwards reads and writes to another db
%%==============================================================================
-module(ecql_replicator).
-behaviour(gen_server).

%% Public API
-export([
   forward/3
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

%% Records
-record(state, {
   result_log, info_log
}).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
forward(Function, Args, Module) ->
  gen_server:cast(?MODULE, {forward, {Function, Args, Module}})
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link() ->
   Opts = [{spawn_opt ,[{max_heap_size ,100000000} ,{message_queue_data ,on_heap}]}]
  ,gen_server:start_link({local, ?MODULE}, ?MODULE, {}, Opts)
.

%%------------------------------------------------------------------------------
init(_) ->
  {ok, #state{result_log = [], info_log = []}}
.

%%------------------------------------------------------------------------------
stop() ->
  gen_server:call(?MODULE, stop)
.

%%------------------------------------------------------------------------------
handle_info(Message, State = #state{info_log = Log}) ->
  {noreply, State#state{info_log = do_log(Log, Message)}}
.

%%------------------------------------------------------------------------------
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}
.

%%------------------------------------------------------------------------------
handle_cast({forward, {Fun, Args, Module}}, State = #state{result_log = Log}) ->
   Result = do_forward(Fun, Args, Module)
  ,{noreply, State#state{result_log = do_log(Log, Result)}}
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
do_forward(query, Args, Module) ->
  do_forward(Args, Module)
;
do_forward(query_async, Args, Module) ->
  do_forward(Args, Module)
;
do_forward(query_batch, Args, {rw, Module}) ->
  do_batch(Module, Args)
;
do_forward(query_batch, Args, Module) ->
  do_batch(Module, Args)
;
do_forward(Other, _Args, _WModule) ->
  {skip, Other}
.

%%------------------------------------------------------------------------------
do_batch(Module, Args) ->
  spawn_link(Module, with_stream_do, [query_batch, Args])
.

%%------------------------------------------------------------------------------
do_forward([Cql, Args, Consistency], {rw, Module}) ->
  Module:with_stream_do(query_async, [Cql, Args, Consistency])
;
do_forward([Cql, Args, Consistency], Module) ->
   Command = binary:bin_to_list(iolist_to_binary(Cql), {0, 4})
  ,Forward = case string:uppercase(Command) of
    "DROP" -> true;
    "CREA" -> true;
    "TRUN" -> true;
    "UPDA" -> true;
    "INSE" -> true;
    "DELE" -> true;
    _AllElse -> false
  end
  ,Forward andalso Module:with_stream_do(query_async, [Cql, Args, Consistency])
.

%%------------------------------------------------------------------------------
do_log(Log, Message) when length(Log) > 200 ->
  [Message | lists:sublist(Log, 50)]
;
do_log(Log, Message) ->
  [Message | Log]
.

%%==============================================================================
%% END OF FILE
