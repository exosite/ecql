%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_replicator_shadow.erl - Replicator that forwards reads and writes
%%==============================================================================
-module(ecql_replicator_shadow).
-behaviour(gen_server).

%% OTP gen_server
-export([
   init/1
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
   queue
}).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
init(_) ->
  {ok, #state{
     queue = queue:new()
  }}
.

%%------------------------------------------------------------------------------
handle_info(set_worker_heap, State) ->
   MaxHeapSize = ecql:config(worker_max_heap_size)
  ,process_flag(max_heap_size, MaxHeapSize)
  ,{noreply, State}
;

%%------------------------------------------------------------------------------
handle_info({forward ,ReqId ,Args}, State) ->
   {noreply, consume_queue({forward ,ReqId ,Args}, State)}
;

%%------------------------------------------------------------------------------
handle_info(process_queue, State = #state{queue = Queue0}) ->
  case queue:out(Queue0) of
    {empty, Queue0} ->
      {noreply, State}
    ;
    {{value, {forward ,ReqId ,Args}}, Queue} ->
       gen_server:cast(ecql_replicator2, {reply, ReqId, do_forward(Args)})
      ,self() ! process_queue
      ,{noreply, State#state{queue = Queue}}
    %~
  end
;

%%------------------------------------------------------------------------------
handle_info(done, State = #state{queue = Queue0}) ->
  case queue:out(Queue0) of
    {empty, Queue0} ->
      {stop, normal, State}
    ;
    {{value, {forward ,ReqId ,Args}}, Queue} ->
       gen_server:cast(ecql_replicator2, {reply, ReqId, do_forward(Args)})
      ,handle_info(done, State#state{queue = Queue})
    %~
  end
.

%%------------------------------------------------------------------------------
handle_call(_What, _From, State) ->
   {reply, error, State}
.

%%------------------------------------------------------------------------------
handle_cast(_What, State) ->
   {noreply, State}
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
consume_queue(Item, State0 = #state{queue = Queue}) ->
   State = State0#state{queue = queue:in(Item, Queue)}
  ,receive
    Next = {forward ,_ReqId ,_Args} ->
       consume_queue(Next, State)
    %~
    after 0 ->
       self() ! process_queue
      ,State
    %~
  end
.

%%------------------------------------------------------------------------------
do_forward({Op, Args, Module}) ->
  case Op of
    query -> do_forward(Op, Args, Module);
    query_async -> do_forward(Op, Args, Module);
    query_batch -> do_forward(Op, Args, Module);
    Other -> {skip, Other}
  end
.

%%------------------------------------------------------------------------------
% in rw (and rwv) mode we forward ops directly
do_forward(Op, Args, {rw, Module}) ->
  Module:with_stream_do(Op, Args)
;
% otherwise we only take modifying ops
do_forward(Op, [Cql, Args, Consistency], Module) ->
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
  ,Forward andalso Module:with_stream_do(Op, [Cql, Args, Consistency])
.

%%==============================================================================
%% END OF FILE
