%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_erlcass.erl - Erlcass connector
%%==============================================================================
-module(ecql_erlcass).

%% Includes
-include("ecql.hrl").
-include("erlcass.hrl").

%% Public API
-export([
    with_stream_do/2
]).

%% Private API exposed for MFA apply()
-export([
     do_execute_paged/2
    ,do_execute/3
]).

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Public API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
with_stream_do(release, []) ->
  case get(last_ccon) of
    undefined ->
       ok;
    Ref ->
       gen_server:cast(ecql_erlcass_throttle, {release, Ref})
      ,erase(last_ccon)
    %~
  end
  ,ok
;
with_stream_do(Command, Args) ->
  case get(last_ccon) of
    undefined ->
      put(last_ccon, gen_server:call(ecql_erlcass_throttle, acquire, infinity))
    ;
    _Ref ->
      ok
    %~
  end
  ,do_with_stream_do(Command, Args)
.

%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
%% Private API
%%-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

%%------------------------------------------------------------------------------
do_with_stream_do(query, [Cql, Args, Consistency]) ->
    execute(Cql, Args, Consistency)
;
do_with_stream_do(query_async, [Cql, Args, Consistency]) ->
    execute(Cql, Args, Consistency)
;
do_with_stream_do(query_page, [Cql, Args, Consistency]) ->
     Atom = prepare_statement(Cql, Consistency)
    ,Args1 = lists:map(fun convert_arg/1, Args)
    ,{ok, Statement} = erlcass:bind_prepared_statement(Atom)
    ,ok = erlcass:bind_prepared_params_by_index(Statement, Args1)
    ,do_with_stream_do(query_page, [{Statement, {id, Atom, {Cql, Args}}}])
;
do_with_stream_do(query_page, [{Statement, Atom}]) ->
    case retry(?MODULE, do_execute_paged, [Statement, Atom]) of
        {ok, Heads, Rows} ->
            Names = lists:map(fun({Name, _Type}) ->
                 binary_to_atom(Name, utf8)
            end, Heads)
            ,Values = text_to_list(Heads, Rows)
            ,{{Names, Values}, '$end_of_table'}
        ;
        {ok, Heads, Rows, HasMore} ->
            Names = lists:map(fun({Name, _Type}) ->
                 binary_to_atom(Name, utf8)
            end, Heads)
            ,Values = text_to_list(Heads, Rows)
            ,case HasMore of
                true -> {{Names, Values}, {Statement, Atom}};
                false -> {{Names, Values}, '$end_of_table'}
            end
        ;
        {error, Reason} when is_atom(Reason) ->
            {error, 0, atom_to_list(Reason)};
        {error, Reason} when is_binary(Reason) ->
            {error, 0, binary_to_list(Reason)}
        %~
    end
;
do_with_stream_do(query_page, ['$end_of_table']) ->
    '$end_of_table'
;
do_with_stream_do(query_batch, [Cql, ListOfArgs, Consistency]) ->
     Name = prepare_statement(Cql, Consistency)
    ,Stmts = lists:map(fun(Args) ->
         {ok, Stmt} = erlcass:bind_prepared_statement(Name)
        ,Args1 = lists:map(fun convert_arg/1, Args)
        ,ok = erlcass:bind_prepared_params_by_index(Stmt, Args1)
        ,Stmt
    end, ListOfArgs)
    ,retry(
         erlcass
        ,batch_execute
        ,[?CASS_BATCH_TYPE_UNLOGGED, Stmts, [{consistency_level, Consistency}]]
    )
;
do_with_stream_do(foldl, [Fun, Acc, Cql, Args, Consistency]) ->
    Ret = do_with_stream_do(query_page, [Cql, Args, Consistency])
    ,do_foldl(Fun, Acc, Ret)
;
do_with_stream_do(release, []) ->
    case get(last_ccon) of
      undefined ->
         ok;
      Ref ->
         gen_server:cast(ecql_erlcass_throttle, {release, Ref})
        ,erase(last_ccon)
      %~
    end
    ,ok
;
do_with_stream_do(sync, []) ->
    ok
.

%%------------------------------------------------------------------------------
do_foldl(Fun, Acc, {{Keys, Rows}, Continuation}) ->
    do_foldl(Fun, Fun(Keys, Rows, Acc), do_with_stream_do(query_page, [Continuation]))
;
do_foldl(_Fun, Acc, '$end_of_table') ->
    Acc
.

%%------------------------------------------------------------------------------
execute(Cql, Args, Consistency) ->
     Atom = prepare_statement(Cql, Consistency)
    ,Args1 = lists:map(fun convert_arg/1, Args)
    ,case retry(?MODULE, do_execute, [{id, Atom, {Cql, Args}}, ?BIND_BY_INDEX, Args1]) of
        ok ->
            ok
        ;
        {ok, Heads, Rows} ->
            Names = lists:map(fun({Name, _Type}) ->
                 binary_to_atom(Name, utf8)
            end, Heads)
            ,Values = text_to_list(Heads, Rows)
            ,{Names, Values}
        ;
        {error, Reason} when is_atom(Reason) ->
            {error, 0, atom_to_list(Reason)};
        {error, Reason} when is_binary(Reason) ->
            {error, 0, binary_to_list(Reason)}
        %~
    end
.

%%------------------------------------------------------------------------------
text_to_list(Heads, Rows) ->
    case lists:any(fun({_Name, Type}) -> is_text(Type) end, Heads) of
        false -> Rows;
        true -> do_text_to_list(Heads, Rows)
    end
.

%%------------------------------------------------------------------------------
do_text_to_list(Heads, Rows) ->
    lists:map(fun(Row) ->
        lists:map(
             fun({{_Name, Type}, Value}) -> convert_value(Type, Value) end
            ,lists:zip(Heads, Row)
        )
    end, Rows)
.

%%------------------------------------------------------------------------------
convert_value(_, null) -> undefined;
convert_value(text, Value) -> binary_to_list(Value);
convert_value({map, text, text}, List) ->
    lists:map(
         fun({Key, Value}) -> {binary_to_list(Key), binary_to_list(Value)} end
        ,List
    )
;
convert_value({map, text, _}, List) ->
    lists:map(
         fun({Key, Value}) -> {binary_to_list(Key), Value} end
        ,List
    )
;
convert_value({map, _, text}, List) ->
    lists:map(
         fun({Key, Value}) -> {Key, binary_to_list(Value)} end
        ,List
    )
;
convert_value(_, Other) ->
    Other
.

%%------------------------------------------------------------------------------
convert_arg(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
convert_arg(Other) -> Other.

%%------------------------------------------------------------------------------
is_text(text) -> true;
is_text({map,text,_}) -> true;
is_text({map,_,text}) -> true;
is_text(_) -> false.

%%------------------------------------------------------------------------------
prepare_statement(Cql, Consistency) ->
   Statement = iolist_to_binary(Cql)
  ,case ets:lookup(ecql_erlcass_statements, Statement) of
    [] ->
         Atom = binary_to_atom(base64:encode(crypto:hash(sha256, Statement)), utf8)
        ,Ret = retry(
           erlcass
          ,add_prepare_statement
          ,[Atom, {Statement, [{consistency_level, convert(Consistency)}]}]
        )
        ,case Ret of
            ok -> ok;
            {error,already_exist} -> ok
        end
        ,ets:insert(ecql_erlcass_statements, {Statement, Atom})
        ,Atom
     ;
    [{Statement, Atom}] ->
       Atom
    %~
  end
.

%%------------------------------------------------------------------------------
% Both drivers use the Cassandra internal numbering, no change
% convert(Consistency) -> Consistency.
convert(?CL_ONE) -> ?CASS_CONSISTENCY_ONE;
convert(?CL_TWO) -> ?CASS_CONSISTENCY_TWO;
convert(?CL_THREE) -> ?CASS_CONSISTENCY_THREE;
convert(?CL_QUORUM) -> ?CASS_CONSISTENCY_QUORUM;
convert(?CL_ALL) -> ?CASS_CONSISTENCY_ALL;
convert(?CL_LOCAL_QUORUM) -> ?CASS_CONSISTENCY_LOCAL_QUORUM;
convert(?CL_EACH_QUORUM) -> ?CASS_CONSISTENCY_EACH_QUORUM;
convert(?CL_SERIAL) -> ?CASS_CONSISTENCY_SERIAL;
convert(?CL_LOCAL_SERIAL) -> ?CASS_CONSISTENCY_LOCAL_SERIAL;
convert(?CL_LOCAL_ONE) -> ?CASS_CONSISTENCY_LOCAL_ONE.

%%------------------------------------------------------------------------------
do_execute({id ,Identifier, {OrgCql, OrgParams}}, BindType, Params) ->
    {Time, Ret} = timer:tc(fun() ->
        case erlcass:async_execute(Identifier, BindType, Params) of
            {ok, Tag} -> receive_response(Tag);
            Error -> Error
        end
    end),
    ecql_log:log(Time, query, OrgCql, OrgParams),
    Ret
.

%%------------------------------------------------------------------------------
% same as in erclass.erl but without timeout
receive_response(Tag) ->
    receive
        {execute_statement_result, Tag, Result} -> Result
    end
.

%%------------------------------------------------------------------------------
do_execute_paged(Stm, {id ,Identifier, {OrgCql, OrgParams}}) ->
    ok = erlcass:set_paging_size(Stm, 1000),
    {Time, Ret} = timer:tc(fun() ->
        case erlcass:async_execute_paged(Stm, Identifier) of
            {ok, Tag} -> receive_paged_response(Tag);
            Error -> Error
        end
    end),
    ecql_log:log(Time, paged, OrgCql, OrgParams),
    Ret
.

%%------------------------------------------------------------------------------
% same as in erlcass.erl but without timeout
receive_paged_response(Tag) ->
    receive
        {execute_statement_result, Tag, Result} -> Result
    end
.

%%------------------------------------------------------------------------------
retry(M, F, A) ->
    retry({M, F, A}, 0)
.

%%------------------------------------------------------------------------------
retry(Mfa = {M, F, A}, N) ->
    case apply(M, F, A) of
        {error, <<"No hosts available", _/binary>> = Msg} ->
            do_retry(Mfa, N, Msg)
    ;
        {error, <<"All hosts", _/binary>> = Msg} ->
            do_retry(Mfa, N, Msg)
    ;
        Ret ->
            Ret
    %~
  end
.

%%------------------------------------------------------------------------------
do_retry(Mfa, N, Msg) ->
    case config_default(retry_limit, 1) of
        Limit when N < Limit ->
            error_logger:error_msg(
                "ecql_erlcass: Query failed with ~p retry: ~p",
                [N, Msg]
            ),
            timer:sleep(config_default(retry_sleep, 200)),
            retry(Mfa, N + 1)
        ;
        _ ->
            error_logger:error_msg(
                "ecql_erlcass: Query failed with ~p aborting",
                [Msg]
            ),
            {error, Msg}
        %~
    end
.

%%------------------------------------------------------------------------------
config_default(Key, Default) ->
    case ecql:config(Key) of
        undefined -> Default;
        Other -> Other
    end
.

%%==============================================================================
%% END OF FILE
