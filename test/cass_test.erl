-module(cass_test).
-export([init/0]).
-export([test/2]).
-export([start_app/2]).

init() ->
   SaslOpt = [{sasl_error_logger, {file, "sasl.log"}}]
  ,start_app(sasl, SaslOpt)
  ,EcqlOpt = [{replication_strategy, "SimpleStrategy"}, {replication_factor, 1}, {keyspace, "test"}]
  ,start_app(ecql, EcqlOpt)
.

test(Module, Inserts) ->
   Module:init(Inserts)
  ,R = do_test(Module, Inserts)
  ,io:format("~p ~p inserts: ~p seconds~n", [Module, Inserts, R/1000000])
.

do_test(Module, Inserts) ->
   {Time, _} = timer:tc(Module, test, [Inserts])
  ,Time
.

start_app(App, Options) ->
   application:load(App)
  ,[application:set_env(App, Key, Value) || {Key, Value} <- Options]
  ,{Ret, _} = application:ensure_all_started(App)
  ,Ret == ok orelse Ret == already_started
.

