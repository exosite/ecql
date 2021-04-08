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

Module Configuration
------------------

The `module` configuration parameter supports four difference values:

* `ecql_erlcass` - all reads+writes handled by the erlcass Cassandra >=2.1 driver.
* `ecql_native` - all reads+writes handled by the ecql internal Cassandra <=2.0 driver.
* `undefined` - same as `ecql_native`
* `{_Primary, _Secondary}` - The first module is used for all reads+writes, the second module receives a copy of ONLY write actions
    * `{ecql_native, ecql_erlcass}`
    * `{ecql_erlcass, ecql_native}`
* `{_Primary, {rw, _Secondary}}` - The first module is used for all reads+writes, the second module receives a copy of each write AND each read action
    * `{ecql_native, {rw, ecql_erlcass}}`
    * `{ecql_erlcass, {rw, ecql_native}}`

* `{_Primary, {rwv, _Secondary}}` - Same as rw but adds validation to the read duplicated result

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

* `?CL_ONE` - Consistency level one.
* `?CL_TWO` - Consistency level two.
* `?CL_THREE` - Consistency level three.
* `?CL_QUORUM` - Consistency level quorum.
* `?CL_ALL` - Consistency level quorum.
* `?CL_LOCAL_QUORUM` - Consistency level local quorum.
* `?CL_EACH_QUORUM` - Consistency level each quorum.
* `?CL_SERIAL` - Consistency level serial.
* `?CL_LOCAL_SERIAL` - Consistency level local serial.
* `?CL_LOCAL_ONE` - Consistency level local one.

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
* Username / Password Auth
* Support for some data types like boolean, decimal, double, timeuuid, inet is
incomplete.
* Replace encode and decode functions with nifs for performance.
* Handle connections to different servers.

