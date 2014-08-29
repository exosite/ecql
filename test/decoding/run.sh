#!/bin/bash

echo
echo "Don't forget to fix your cpu speed! e.g. 'sudo cpufreq-set -f 1700000'"
echo
# sudo cpufreq-set -f 1700000

erl -make
str=ok
for file in ecql_decoding*; do
  mod=${file%.*}
  str="$str,$mod:test(100000)"
done
erl -noshell -eval "$str,init:stop()."
