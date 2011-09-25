#!/bin/bash
hadoop fs -rmr ./wcoutput 
hadoop fs -ls .
echo "==================== Will trigger the job now ==================="
hadoop jar `dirname $0`/../g4h/target/g4h-0.0.1-SNAPSHOT.jar $*
echo "==================== Job Done ==================================="
hadoop fs -ls ./wcoutput
echo "======================= Results ================================="
#hadoop fs -cat ./wcoutput/part*
