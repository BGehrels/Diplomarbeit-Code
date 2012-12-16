#!/bin/bash

shopt -s nullglob
declare -a algos=(import readWholeGraph calcSCC calcFoF calcCommonFriends calcRegularPathQueries)
declare -a dbs=(flockdb neo4j hypergraphdb)

function runBenchmarkStep() {
	LOGFILENAME="logs/$1-$2-$3.stdout"
	TIMEFILENAME="logs/$1-$2-$3.stderr"

	if [[ ! -a  "$LOGFILENAME" ]]
	then
		echo "running $3 for $2 on $1"

		java -Xmx1g -jar target/benchmark-1.0-jar-with-dependencies.jar geoff/$1 $2 $3 1> "$LOGFILENAME" 2> "$TIMEFILENAME"
		RESULT = $?
		cat $TIMEFILENAME

		if [[ $RESULT != 0 ]] ; then
			echo "ERROR"
			exit
		fi

		rebootToClearCaches
	fi

}

function rebootToClearCaches() {
	echo "-------------------"
}

function clearAllDatabaseTmpFiles() {
	LOGFILENAME="logs/$1.stdout"

	if [[ ! -a  "$LOGFILENAME" ]]
	then
		echo "clearing tmp files for $1"
		rm -Rf neo4jDB
		rm -Rf hyperGraphDB
		echo "DROP DATABASE edges_development" | mysql -u root
		echo "DROP DATABASE flockdb_development" | mysql -u root

		echo "Param: " $2
		if [[ $2 == "dev" ]]
		then
			./startFlockDB.sh
		fi

		rebootToClearCaches
	fi
}

function runBenchmark() {
	for algo in ${algos[@]}
	do
	    runBenchmarkStep $1 $2 $algo
	done
}


function compareLogs() {
	# args: 1: dataFileName, 2: db1, 3: db2
	for algo in ${algos[@]}
	do
		compareLogsForAlgo $1 $algo $2 $3
	done
}

function compareLogsForAlgo() {
	# args: 1: dataFileName, 2: algo, 3: db1, 4: db2
	sort logs/$1-$3-$2.stdout | grep -v "checkpoint .*:0" > logs/$1-$3-$2.sorted
	sort logs/$1-$4-$2.stdout | grep -v "checkpoint .*:0" > logs/$1-$4-$2.sorted
	diff -q logs/$1-$3-$2.sorted logs/$1-$4-$2.sorted
	if [[ $? != 0 ]] ; then
        echo logs/$1-$3-$2.sorted and logs/$1-$4-$2.sorted were different
        exit
    fi
}

for f in geoff/*.geoff
do
	echo "Running benchmark for $f";

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
    clearAllDatabaseTmpFiles $f $1
done;