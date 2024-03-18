#!/bin/bash
rm /home/jjoriping/pprof/* -r # clean up data from previous runs
rm cpu_profile.log

PROCESS_NAME=$1

echo "CPU profiling for $PROCESS_NAME"
while true; do
  VAL=$(top -b -n 1 | awk 'NR>7{ sum += $9; } END{ print sum; }')
  echo "$(date) $VAL" >> cpu_profile.log
  sleep 1
done
# while true; do
#   go tool pprof -output temp.log -text http://localhost:6060/debug/pprof/profile?seconds=10
#   date >> cpu_profile.log
#   cat temp.log >> cpu_profile.log
#   echo "-------------------------------------" >> cpu_profile.log
# done
# mpstat 1 > cpu_profile.log
