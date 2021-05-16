#!/bin/bash
# run all sorting centers at the same time
export CLASSPATH=`pwd`/jar/\*:/home/bkc/src/3rdParty/pravega-client-0.9.0/\* 
SCOPE=test
COMMON_ARGS="-r -u tcp://192.168.198.4:9090 --scope $SCOPE --rs localhost --wait_for_events --mark 1000 -l debug"

echo "starting 4 sorting-center processors"
jython sorting_center.py $COMMON_ARGS -s A &
jython sorting_center.py $COMMON_ARGS -s B & 
jython sorting_center.py $COMMON_ARGS -s C & 
jython sorting_center.py $COMMON_ARGS -s D --report_lost_packages &

wait
echo "processing completed"
