#!/bin/bash
#rm -Rf `dirname $0`/../g4h/target
cd `dirname $0`/../g4h/
mvn clean
rm -Rf `dirname $0`/../g4happs/bin
cd ..
hg commit -A

