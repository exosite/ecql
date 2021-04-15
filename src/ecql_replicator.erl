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
  ,forward/4
  ,pending_queries/0
  ,flush_pending_queries/0
  ,set_heap/1
  ,set_worker_heap/1
  ,max_ref_size/0
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
-define(MAX_REF, 100000).
-define(
   ERROR_MSG
  ,"replicator received differing results: query: ~p~n"
   "reference: ~p~n"
   "received : ~p~n"
).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
forward(Function, Args, Module) ->
  gen_server:cast(?MODULE, {forward, self(), {Function, Args, Module}})
.

%%------------------------------------------------------------------------------
forward(Function, Args, Module, Ref) ->
  gen_server:cast(?MODULE, {forward, self(), {Function, Args, Module}, Ref})
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

%%------------------------------------------------------------------------------
max_ref_size() ->
  unless(ecql:config(max_ref_size), ?MAX_REF)
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
flush_pending_queries() ->
  gen_server:cast(?MODULE, flush_pending_queries)
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
handle_cast(flush_pending_queries, State) ->
  {noreply, State#state{pending_queries = #{}}}
;

%%------------------------------------------------------------------------------
handle_cast({forward, From, Args, Ref}, #state{pending_queries = Queries} = State) ->
   ReqId = make_ref()
  ,State1 = handle_forward(ReqId, From, Args, State)
  ,{noreply, State1#state{pending_queries = maps:put(ReqId, {Args, Ref}, Queries)}}
;

%%------------------------------------------------------------------------------
handle_cast({forward, From, Args}, #state{pending_queries = Queries} = State) ->
   ReqId = make_ref()
  ,State1 = handle_forward(ReqId, From, Args, State)
  ,{noreply, State1#state{pending_queries = maps:put(ReqId, Args, Queries)}}
;

%%------------------------------------------------------------------------------
% these are reads in simple write replication mode
handle_cast({reply, ReqId, false}, State) ->
   #state{pending_queries = Queries} = State
  ,{noreply, State#state{
     pending_queries = maps:remove(ReqId, Queries)
  }}
;

%%------------------------------------------------------------------------------
% these are continuations and other driver specific calls we can't replicate
handle_cast({reply, ReqId, {skip, _}}, State) ->
   #state{pending_queries = Queries} = State
  ,{noreply, State#state{
     pending_queries = maps:remove(ReqId, Queries)
  }}
;

%%------------------------------------------------------------------------------
handle_cast({reply, ReqId, Result}, State) ->
   #state{pending_queries = Queries, result_log = Log} = State
  ,case maps:get(ReqId, Queries, undefined) of
    {_, Result} -> ok;
    {Args, Ref} -> error_logger:info_msg(?ERROR_MSG, [Args, Ref, Result]);
    _Other -> ok
  end
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
      ,loop_shadow()
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
handle_forward(ReqId, From, Args, #state{shadows = Shadows} = State) ->
  State1 = case maps:get(From, Shadows, undefined) of
    undefined ->
       Pid = start_shadow()
      ,monitor(process, From)
      ,State#state{shadows = maps:put(From, Pid, Shadows)}
    ;
    APid ->
      case is_process_alive(APid) of
        true ->
           Pid = APid
          ,State
        ;
        false ->
           Pid = start_shadow()
          ,State#state{shadows = maps:put(From, Pid, Shadows)}
        %~
      end
    %~
  end
  ,Pid ! {forward, ReqId, Args}
  ,State1
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

%%------------------------------------------------------------------------------
do_log(Log, Message) when length(Log) > 200 ->
  [Message | lists:sublist(Log, 50)]
;
do_log(Log, Message) ->
  [Message | Log]
.

%%==============================================================================
%% END OF FILE
