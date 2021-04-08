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
  ,pending_queries/0
  ,set_heap/1
  ,set_worker_heap/1
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
   shadows, info_log, result_log, pending_queries
}).

%% Defines
-define(HEAP, 10000000).
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
forward(Function, Args, Module) ->
  gen_server:cast(?MODULE, {forward, self(), {Function, Args, Module}})
.


%%------------------------------------------------------------------------------
% in addition to setting the ecql:config() value this also send a message
% to the replicator to adjust its max heap at runtime
set_heap(MaxHeapSize) when is_integer(MaxHeapSize) ->
  gen_server:cast(?MODULE, {set_heap, MaxHeapSize})
.

%%------------------------------------------------------------------------------
% in addition to setting the ecql:config() value this also send a message
% to all workers which then will adjust their max heap size
set_worker_heap(MaxHeapSize) when is_integer(MaxHeapSize) ->
  gen_server:cast(?MODULE, {set_worker_heap, MaxHeapSize})
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link() ->
   Heap = unless(ecql:config(replicator_heap_size), 100000000)
  ,Opts = [{spawn_opt ,[{max_heap_size ,Heap} ,{message_queue_data ,on_heap}]}]
  ,gen_server:start_link({local, ?MODULE}, ?MODULE, {}, Opts)
.

%%------------------------------------------------------------------------------
init(_) ->
  {ok, #state{
     shadows = #{}
    ,info_log = []
    ,result_log = []
    ,pending_queries = #{}
  }}
.

%%------------------------------------------------------------------------------
stop() ->
  gen_server:call(?MODULE, stop)
.

%%------------------------------------------------------------------------------
pending_queries() ->
  gen_server:call(?MODULE, pending_queries)
.

%%------------------------------------------------------------------------------
handle_info({'DOWN', _Ref, process, Pid, _Info}, State) ->
   maps:get(Pid, State#state.shadows) ! done
  ,{noreply, State#state{
     shadows = maps:remove(Pid, State#state.shadows)
  }}
;

%%------------------------------------------------------------------------------
handle_info(Message, State = #state{info_log = Log}) ->
  {noreply, State#state{info_log = do_log(Log, Message)}}
.


%%------------------------------------------------------------------------------
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}
;

%%------------------------------------------------------------------------------
handle_call(pending_queries, _From, State) ->
  {reply, maps:size(State#state.pending_queries), State}
.

%%------------------------------------------------------------------------------
handle_cast({forward, From, Args}, State) ->
   #state{shadows = Shadows, pending_queries = Queries} = State
  ,ReqId = make_ref()
  ,State1 = case maps:get(From, Shadows, undefined) of
    undefined ->
       Pid = start_shadow()
      ,monitor(process, From)
      ,State#state{shadows = maps:put(From, Pid, Shadows)}
    ;
    Pid ->
       State
    %~
  end
  ,Pid ! {forward, ReqId, Args}
  ,{noreply, State1#state{pending_queries = maps:put(ReqId, Args, Queries)}}
;

%%------------------------------------------------------------------------------
handle_cast({reply, ReqId, Result}, State) ->
   #state{pending_queries = Queries, result_log = Log} = State
  ,{noreply, State#state{
     pending_queries = maps:remove(ReqId, Queries)
    ,result_log = do_log(Log, Result)
  }}
;

%%------------------------------------------------------------------------------
handle_cast({set_heap, MaxHeapSize}, State) ->
   ecql:config(replicator_heap_size, MaxHeapSize)
  ,process_flag(max_heap_size, MaxHeapSize)
  ,{noreply, State}
;

%%------------------------------------------------------------------------------
handle_cast({set_worker_heap, MaxHeapSize}, State = #state{shadows = Pids}) ->
   ecql:config(worker_max_heap_size, MaxHeapSize)
  ,lists:foreach(fun(Pid) -> Pid ! set_worker_heap  end, maps:values(Pids))
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
unless(undefined, Default) -> Default;
unless(Other, _Default) -> Other.

%%------------------------------------------------------------------------------
start_shadow() ->
   % default can only be used when through a hot-upgrade the term is not set
   MaxHeapSize = unless(ecql:config(worker_max_heap_size), ?HEAP)
  ,spawn_opt(
     fun loop_shadow/0
    ,[link, {max_heap_size ,MaxHeapSize} ,{message_queue_data ,on_heap}]
  )
.

%%------------------------------------------------------------------------------
loop_shadow() ->
  receive
    set_worker_heap ->
       MaxHeapSize = ecql:config(worker_max_heap_size)
      ,process_flag(max_heap_size, MaxHeapSize)
      ,ok
    ;
    done ->
      ok
    ;
    {forward, ReqId, Args} ->
       gen_server:cast(?MODULE, {reply, ReqId, do_forward(Args)})
      ,loop_shadow()
    %~
  end
.

%%------------------------------------------------------------------------------
do_forward({query, Args, Module}) ->
  do_forward(Args, Module)
;
do_forward({query_async, Args, Module}) ->
  do_forward(Args, Module)
;
do_forward({query_batch, Args, {rw, Module}}) ->
  do_batch(Module, Args)
;
do_forward({query_batch, Args, Module}) ->
  do_batch(Module, Args)
;
do_forward({Other, _Args, _WModule}) ->
  {skip, Other}
.

%%------------------------------------------------------------------------------
do_batch(Module, Args) ->
  Module:with_stream_do(query_batch, Args)
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
