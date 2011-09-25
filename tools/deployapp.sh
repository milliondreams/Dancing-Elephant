#!/bin/sh
if [ $# -eq 1 ]
then
	hadoop fs -rmr ./g4happs/$1
	hadoop fs -copyFromLocal `dirname $0`/../g4happs/$1/ ./g4happs/$1/
#	hadoop fs -copyFromLocal ./g4happs/$1/conf ./g4happs/$1/conf
	hadoop fs -ls ./g4happs/$1
else
	echo "\t\tSyntax:"
	echo "\t\t\tsh $0 <appname>"
	echo "Goodbye!"
	exit 1
fi
echo "Done!"
