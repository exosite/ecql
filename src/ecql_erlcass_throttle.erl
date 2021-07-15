%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_erlcass_throttle.erl - GenServer to limit the paralelism of ecql_erlcass
%%==============================================================================
-module(ecql_erlcass_throttle).
-behaviour(gen_server).

%% Public API
-export([
   pending_queries/0
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

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
pending_queries() ->
  gen_server:call(?MODULE ,pending_queries, 1000)
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {}, [])
.

%%------------------------------------------------------------------------------
init(_) ->
   timer:send_interval(5000, update)
  ,Max = unless(ecql:config(erlcass_max_processes), 600)
  ,{ok, #{max_processes => Max, refs => [], waiting => queue:new()}}
.

%%------------------------------------------------------------------------------
stop() ->
  gen_server:call(?MODULE, stop)
.

%%------------------------------------------------------------------------------
handle_call(pending_queries, _From, State = #{waiting := Queue}) ->
   {reply, queue:len(Queue) ,State}
;
handle_call(acquire, From, State = #{waiting := Queue}) ->
   {noreply, process_waiting(State#{waiting => queue:in(From, Queue)})}
.

%%------------------------------------------------------------------------------
handle_cast({release, Ref}, State) ->
   {noreply, process_waiting(free(Ref, State))}
.

%%------------------------------------------------------------------------------
handle_info({'DOWN', Ref, process, _Pid, _Info}, State) ->
   {noreply, process_waiting(free(Ref, State))}
;

%%------------------------------------------------------------------------------
handle_info(update, State) ->
   Max = unless(ecql:config(erlcass_max_processes), 600)
  ,{noreply, process_waiting(State#{max_processes => Max})}
;

%%------------------------------------------------------------------------------
handle_info(Other, State) ->
   error_logger:error_msg("ecql_erlcass_throttle: Received ~p", [Other])
  ,{noreply, State}
.

%%------------------------------------------------------------------------------
terminate(_Reason, State) ->
  {shutdown, State}
.

%%------------------------------------------------------------------------------
code_change(_, State, _) ->
  {ok, State}
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Private API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
unless(undefined, Default) -> Default;
unless(Other, _Default) -> Other.

%%------------------------------------------------------------------------------
free(Ref, State = #{refs := Refs}) ->
  State#{refs => lists:delete(Ref, Refs)}
.


%%------------------------------------------------------------------------------
process_waiting(
  State = #{max_processes := Max, refs := Refs}
) when length(Refs) >= Max ->
  State
;

%%------------------------------------------------------------------------------
process_waiting(State = #{refs := Refs, waiting := Queue} = State) ->
  case queue:out(Queue) of
    {{value ,From}, Rest} ->
       {Pid ,_Tag} = From
      ,Ref = monitor(process, Pid)
      ,gen_server:reply(From, Ref)
      ,State#{waiting => Rest, refs => [Ref | Refs]}
    ;
    {empty ,Queue} ->
      State
    %~
  end
.

%%==============================================================================
%% END OF FILE
