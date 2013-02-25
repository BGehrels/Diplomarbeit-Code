#!/bin/bash

shopt -s nullglob
declare -a algos=(import readWholeGraph calcSCC calcFoF calcCommonFriends calcRegularPathQueries)
declare -a dbs=(dex neo4j hypergraphdb flockdb)

function runBenchmarkStep() {
	BZIPED_GEOFF_FILE=$1
	DBMS=$2
	ALGO=$3

	GEOFF_FILE=`basename $1 ".bz2"`
	LOGFILENAME="logs/$GEOFF_FILE-$DBMS-$ALGO.stdout"
	TIMEFILENAME="logs/$GEOFF_FILE-$DBMS-$ALGO.stderr"

	if [[ ! -a  "$LOGFILENAME" ]]
	then
		echo "running $ALGO for $DBMS on $GEOFF_FILE"
		
		if [[ $ALGO == "import" ]]
		then
			echo "unzipping $BZIPED_GEOFF_FILE"
			bunzip2 geoff/$BZIPED_GEOFF_FILE
		fi

		# Every DBMS may use 29 G of RAM.
		if [[ $2 == "hypergraphdb" ]]
		then		
			# since the JVM uses approx. 2 G of overhead, we set -Xmx to 27g by default
			JAVA_MEM="-Xmx27g"
		fi
		
		if [[ $2 == "dex" ]]
		then		
			# since the JVM uses approx. 2 G of overhead, we set -Xmx to 27g by default
			JAVA_MEM="-Xmx6g"
		fi
		
		if [[ $2 == "flockdb" ]] 
		then
			# 6G fuer FlockDB;
			# 6G fuers meinen Benchmarkcode
			# 17G fuer mysql
			JAVA_MEM="-Xmx6g"

			echo "starting Mysql"
			sudo service mysql start

			if [[ $ALGO == "import" ]]
			then
				./createCleanFlockDB.sh
			else
				./startFlockDB.sh
			fi
		fi
		
		if [[ $DBMS == "neo4j" ]] 
		then
			JAVA_MEM="-Xmx22g -XX:+UseConcMarkSweepGC"
			sudo sh -c "echo '50' > /proc/sys/vm/dirty_background_ratio"
			sudo sh -c "echo '90' > /proc/sys/vm/dirty_ratio"
		fi

		java $JAVA_MEM -server -jar target/benchmark-1.0-jar-with-dependencies.jar geoff/$GEOFF_FILE $DBMS $ALGO 1> "$LOGFILENAME" 2> "$TIMEFILENAME"
		RESULT=$?
		grep -v "Picked up JAVA_TOOL_OPTIONS" $TIMEFILENAME

		if [[ $RESULT != 0 ]] ; then
			echo "ERROR"
			exit
		fi

		if [[ $DBMS == "flockdb" ]]
                then
			./stopFlockDB.sh
                        sudo service mysql stop
                fi

		if [[ $ALGO == "import" ]]
		then
			echo "zipping $GEOFF_FILE"
			bzip2 geoff/$GEOFF_FILE
		fi

		clearOSCaches
	fi

}

function clearOSCaches() {
	echo "rebooting..."
	sudo reboot
	exit
}

function clearAllDatabaseTmpFiles() {
	DBMS=$1
	echo "clearing tmp files for $DBMS"

	if [[ $DBMS == "flockdb" ]]
	then
		sudo service mysql start
		echo "DROP DATABASE edges_development" | mysql -u root
		echo "DROP DATABASE flockdb_development" | mysql -u root
		sudo service mysql stop
		sudo sh -c "rm /var/lib/mysql/ib*"
	fi

	if [[ $DBMS == "neo4j" ]]
	then
		rm -Rf neo4jDB
	fi

	if [[ $DBMS == "hypergraphdb"  ]]
	then
		rm -Rf hyperGraphDB
	fi
	
	if [[ $DBMS == "dex" ]]
	then		
		rm benchmark.*
		rm dex.log
	fi
}

function runBenchmark() {
	BZIPPED_FILE_NAME=$1
	DBMS=$2
	for algo in ${algos[@]}
	do
	    runBenchmarkStep $BZIPPED_FILE_NAME $DBMS $algo
	done

	clearAllDatabaseTmpFiles $DBMS
}


function compareLogs() {
	# args: 1: dataFileName, 2: db1, 3: db2
	for algo in ${algos[@]}
	do
		compareLogsForAlgo $1 $algo $2 $3
	done
}

function compareLogsForAlgo() {
	DATA_FILE_NAME=$1
	ALGO=$2
	DBMS1=$3
	DBMS2=$4

	UNZIPED_DATA_FILE_NAME=`basename $1 '.bz2'`
	FILE_BASE_NAME_1=logs/$UNZIPED_DATA_FILE_NAME-$DBMS1-$ALGO
	FILE_BASE_NAME_2=logs/$UNZIPED_DATA_FILE_NAME-$DBMS2-$ALGO

	sort $FILE_BASE_NAME_1.stdout | grep -v "checkpoint .*:0" > $FILE_BASE_NAME_1.sorted 
	sort $FILE_BASE_NAME_2.stdout | grep -v "checkpoint .*:0" > $FILE_BASE_NAME_2.sorted 

	diff -q $FILE_BASE_NAME_1.sorted $FILE_BASE_NAME_2.sorted
	if [[ $? != 0 ]] ; then
		echo $FILE_BASE_NAME_1.sorted and $FILE_BASE_NAME_2.sorted were different
		exit
	fi
}

for f in geoff/*.geoff.bz2
do
	for db in ${dbs[@]}
	do
		runBenchmark `basename $f` $db
	done

	for db1 in ${dbs[@]}
	do
		for db2 in ${dbs[@]}
		do
			compareLogs `basename $f` $db1 $db2
		done
	done


	mv $f geoffDone
done;
