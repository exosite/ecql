%%==============================================================================
%% Copyright (c) Exosite LLC
%%
%% ecql_perf.erl - Connector
%%==============================================================================
-module(ecql_perf).
-export([
  init/1
 ,test/1
]).

% ecql_perf:init(1).
% ecql_perf:test(1).

% ecql_perf:test(2).

init(1) ->
   Table = "CREATE TABLE IF NOT EXISTS record_name (a int PRIMARY KEY, b int) WITH caching = 'none';"
  ,ecql_stream:query(Table)
;

init(2) ->
   Table2 = "CREATE TABLE IF NOT EXISTS record_name2 (a int PRIMARY KEY, b int) WITH
    bloom_filter_fp_chance=0.010000 AND
    caching='KEYS_ONLY' AND
    comment='' AND
    dclocal_read_repair_chance=0.000000 AND
    gc_grace_seconds=864000 AND
    index_interval=128 AND
    read_repair_chance=0.100000 AND
    replicate_on_write='true' AND
    populate_io_cache_on_flush='false' AND
    default_time_to_live=0 AND
    speculative_retry='99.0PERCENTILE' AND
    compaction={'class': 'LeveledCompactionStrategy'} AND
    memtable_flush_period_in_ms=3600000 AND
    compression={'sstable_compression': 'LZ4Compressor'};"
  ,ecql:execute(Table2)
.

test(1) ->
  timer:tc(fun() ->
    lists:foreach(
      fun(Each) ->
         ecql:execute_async("INSERT INTO record_name (a, b) VALUES(?, ?)", [Each, Each])
        ,Each = ecql:select_value("SELECT b FROM record_name WHERE a = ?", [Each])
      end
     ,lists:seq(1, 50000)
    )
  end)
;

test(2) ->
  timer:tc(fun() ->
    lists:foreach(
      fun(Each) ->
         ecql:execute("INSERT INTO record_name2 (a, b) VALUES(?, ?)", [Each, Each])
        ,Each = ecql:select_value("SELECT b FROM record_name2 WHERE a = ?", [Each])
      end
     ,lists:seq(1, 50000)
    )
  end)
.

%%==============================================================================
%% END OF FILE
