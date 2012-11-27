#!/bin/bash

shopt -s nullglob


function runBenchmarkStep() {
	JAVACP="target/classes;C:/Users/Benjamin/.m2/repository/org/neo4j/neo4j/1.8.RC1/neo4j-1.8.RC1.jar;C:/Users/Benjamin/.m2/repository/org/neo4j/neo4j-kernel/1.8.RC1/neo4j-kernel-1.8.RC1.jar;C:/Users/Benjamin/.m2/repository/org/apache/geronimo/specs/geronimo-jta_1.1_spec/1.1.1/geronimo-jta_1.1_spec-1.1.1.jar;C:/Users/Benjamin/.m2/repository/org/neo4j/neo4j-lucene-index/1.8.RC1/neo4j-lucene-index-1.8.RC1.jar;C:/Users/Benjamin/.m2/repository/org/apache/lucene/lucene-core/3.5.0/lucene-core-3.5.0.jar;C:/Users/Benjamin/.m2/repository/org/neo4j/neo4j-graph-algo/1.8.RC1/neo4j-graph-algo-1.8.RC1.jar;C:/Users/Benjamin/.m2/repository/org/neo4j/neo4j-udc/1.8.RC1/neo4j-udc-1.8.RC1.jar;C:/Users/Benjamin/.m2/repository/org/neo4j/neo4j-graph-matching/1.8.RC1/neo4j-graph-matching-1.8.RC1.jar;C:/Users/Benjamin/.m2/repository/org/neo4j/neo4j-cypher/1.8.RC1/neo4j-cypher-1.8.RC1.jar;C:/Users/Benjamin/.m2/repository/org/scala-lang/scala-library/2.9.1-1/scala-library-2.9.1-1.jar;C:/Users/Benjamin/.m2/repository/org/neo4j/neo4j-jmx/1.8.RC1/neo4j-jmx-1.8.RC1.jar;C:/Users/Benjamin/.m2/repository/com/google/guava/guava/13.0-rc2/guava-13.0-rc2.jar;C:/Users/Benjamin/.m2/repository/org/apache/commons/commons-io/1.3.2/commons-io-1.3.2.jar;C:/Users/Benjamin/.m2/repository/info/gehrels/FlockDBClient/0.1-SNAPSHOT/FlockDBClient-0.1-SNAPSHOT.jar;C:/Users/Benjamin/.m2/repository/org/apache/thrift/libthrift/0.8.0/libthrift-0.8.0.jar;C:/Users/Benjamin/.m2/repository/org/slf4j/slf4j-api/1.7.2/slf4j-api-1.7.2.jar;C:/Users/Benjamin/.m2/repository/commons-lang/commons-lang/2.5/commons-lang-2.5.jar;C:/Users/Benjamin/.m2/repository/org/apache/httpcomponents/httpclient/4.1.2/httpclient-4.1.2.jar;C:/Users/Benjamin/.m2/repository/org/apache/httpcomponents/httpcore/4.1.3/httpcore-4.1.3.jar;C:/Users/Benjamin/.m2/repository/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar;C:/Users/Benjamin/.m2/repository/commons-codec/commons-codec/1.4/commons-codec-1.4.jar;C:/Users/Benjamin/.m2/repository/org/slf4j/slf4j-nop/1.7.2/slf4j-nop-1.7.2.jar;C:/Users/Benjamin/.m2/repository/mysql/mysql-connector-java/5.1.21/mysql-connector-java-5.1.21.jar"
	LOGFILENAME="logs/$1-$2-$3.stdout"
	TIMEFILENAME="logs/$1-$2-$3.stderr"

	if [[ ! -a  "$LOGFILENAME" ]]
	then
		echo "running $3 for $2 on $1"

		java -Xmx1g -classpath "$JAVACP" -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044 info.gehrels.diplomarbeit.RunBenchmarkStep geoff/$1 $2 $3 1> "$LOGFILENAME" 2> "$TIMEFILENAME"
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
		echo "DROP DATABASE edges_development" | mysql -u root
		echo "DROP DATABASE flockdb_development" | mysql -u root

		echo "Param: " $2
		if [[ $2 == "dev" ]]
		then
			cd ../../Software/FlockDB/flockdb
			./dist/flockdb/scripts/setup-env.sh
			cd ../../../Code/benchmark/
		fi

		rebootToClearCaches
	fi
}

function runBenchmark() {
    runBenchmarkStep $1 $2 "import"
    runBenchmarkStep $1 $2 "readWholeGraph"
	runBenchmarkStep $1 $2 "calcSCC"
    runBenchmarkStep $1 $2 "calcFoF"
    runBenchmarkStep $1 $2 "calcCommonFriends"
    runBenchmarkStep $1 $2 "calcRegularPathQueries"
}

for f in geoff/*.geoff
do
	echo "Running benchmark for $f";

	for db in flockdb neo4j
	do
		runBenchmark `basename $f` $db
	done


	mv $f geoffDone
    clearAllDatabaseTmpFiles $f $1
done;