#!/bin/bash

if [ "$JAVA_HOME" = "" ]; then
	JAVA_HOME=/opt/jdk1.7.0_79
fi
if [ "$HADOOP_HOME" = "" ]; then
	HADOOP_HOME=/hadoop/hadoop-2.7.1
fi
JAVA_OPTS="-Xmx1024M"

JAR=`dirname $0`
JAR="$JAR/hdftp.jar"
JAVA_CMD="$JAVA_HOME/bin/java"

pid=/var/run/hdftp.pid

usage="Usage: hdftp.sh (start | stop | adduser)"
cmd=$1

case $cmd in

	(start)

		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo HdFtp running as process `cat $pid`.  Stop it first.
				exit 1
			fi
		fi

		echo Starting HdFtp ...
		$JAVA_CMD ${JAVA_OPTS} -jar ${JAR} "$HADOOP_HOME" & echo $! > $pid
		;;

	(stop)

		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo Stopping HdFtp ...
				kill `cat $pid`
				rm $pid
			else
				echo No HdFtp to stop
			fi
		else
			echo No HdFtp to stop
		fi
		;;

	(adduser)

		CLASSPATH=$JAR
		for f in ../lib/*.jar;do
			CLASSPATH=${CLASSPATH}:$f;
		done
		$JAVA_CMD  -classpath ${CLASSPATH} org.apache.hadoop.hdftp.HdFtpAddUser
		;;

	(*)
		echo $usage
		exit 1
		;;
esac
