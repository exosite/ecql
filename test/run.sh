#!/bin/bash

echo
echo "Don't forget to fix your cpu speed! e.g. 'sudo cpufreq-set -f 1700000'"
echo
# sudo cpufreq-set -f 1700000

erl -make
#export ERL_LIBS=../ebin
str='true=code:add_path("../ebin"),application:start(cass_test),cass_test:init()'
for file in ecql_*; do
  mod=${file%.*}
  str="$str,cass_test:test($mod,50000)"
done
echo erl -noshell -eval "$str,init:stop()."
erl -noshell -eval "$str,init:stop()."
