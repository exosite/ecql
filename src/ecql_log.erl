%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_log.erl - Cache
%%==============================================================================
-module(ecql_log).
-behaviour(gen_server).

%% Public API
-export([
   log/1
  ,log/2
  ,set_file/1
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

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
set_file(Filename) ->
  gen_server:call(?MODULE, {set_file, Filename})
.

%%------------------------------------------------------------------------------
log(Format) ->
  log(Format, [])
.

%%------------------------------------------------------------------------------
log(Format, Args) ->
  gen_server:cast(?MODULE, {log, Format, Args})
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% OTP gen_server API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, {} ,[])
.

%%------------------------------------------------------------------------------
init(_) ->
   {ok, LogFile} = application:get_env(log)
  ,case LogFile of
    Filename when is_list(Filename) ->
      {ok, Fp} = file:open(Filename, [binary, delayed_write, append])
    ;
    _ ->
      Fp = none
  end
  ,{ok, Fp}
.

%%------------------------------------------------------------------------------
stop() ->
  gen_server:call(?MODULE, stop)
.

%%------------------------------------------------------------------------------
handle_call({set_file, Filename}, _From, State) ->
  case State of
     none -> ok
    ;Fp -> file:close(Fp)
  end
  ,case file:open(Filename, [binary, delayed_write, append]) of
     {ok, Fp2} -> {reply, ok, Fp2}
    ;Other -> {reply, Other, none}
  end
;
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}
.

%%------------------------------------------------------------------------------
handle_cast({log, Format, Args} ,State) ->
   (State /= none) andalso begin
     io:format(State ,Format ,Args)
   end
  ,{noreply ,State}
;
handle_cast(terminate ,State) ->
  {stop ,terminated ,State}
.

%%------------------------------------------------------------------------------
handle_info(timeout, State) ->
   % Who timed out?
   error_logger:error_msg("ecql_cache: Timeout occured~n")
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

%%==============================================================================
%% END OF FILE
