#!/bin/bash
# download act log from start to end
start="2017-03-23"
end="2017-07-30"
duration=$(( ($(date --date=${end} +%s) - $(date --date=${start} +%s) )/(60*60*24) ))
for i in `seq 0 ${duration}`
do
  day=$(date -d "${start} $i days" +%m%d)
  echo "Downloading ${day} data"
  bash downloadAct.sh ${day}
done