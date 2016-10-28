ecql and ecql_mnesia - Cassandra driver for Erlang.
============================

A driver for cassandras native protocol. This driver uses cassandras stream
feature to create independent worker processes and avoid deadlocking.
Rather than exposing all features of cassandra ease of use and performance are
the main goals of this driver.
Additionally this driver includes a drop-in replacement for mnesia to make
a transition from mnesia to cassandra easier.

Usage
-----

ecql is a simple API.



`ecql:execute/1,2,3`
------------------

* `ecql:execute(Cql)`
* `ecql:execute(Cql, Arguments)`
* `ecql:execute(Cql, Arguments, Consistency)`

where CQL is a valid CQL statement. See here for CQL documentation:
http://cassandra.apache.org/doc/cql3/CQL.html

Arguments are the arguments to be replaced in the CQL string. When a non-empty
arguments lists is provided ecql internally creates and caches a prepared 
query for execution.

Consistency is the consistency level. The valid values are:

* `default` - Alias for local_quorum and default value when no consistency is 
              provied.
* `one` - Consistency level one.
* `two` - Consistency level two.
* `three` - Consistency level three.
* `quorum` - Consistency level quorum.
* `all` - Consistency level quorum.
* `local_quorum` - Consistency level local quorum.
* `each_quorum` - Consistency level each quorum.
* `serial` - Consistency level serial.
* `local_serial` - Consistency level local serial.
* `local_one` - Consistency level local one.

`ecql:execute/1,2,3`
------------------

* `ecql:execute_async(Cql)`
* `ecql:execute_async(Cql, Arguments)`
* `ecql:execute_async(Cql, Arguments, Consistency)`

Same as `ecql:execute/1` but does not wait for the return value. Internally
the number of running asynchronous calls is counted and limit to 100 per calling
process. If the number exceeds this limit the calling process is blocked until 
at least one asynchronous query finishes.

`ecql:sync/0`
------------------

* `ecql:sync()`

Waits for all asynchronous queries to finish.

Todo List (open for contributions)
-----------

* SSL
* Be more resilent to cassandra disconnects restarts.
* Support for some data types like boolean, decimal, double, timeuuid, inet is
incomplete.
* Replace encode and decode functions with nifs for performance.

