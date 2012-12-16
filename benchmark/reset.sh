#!/bin/bash

if [[ $1 == "full" ]]
then
	mv geoffDone/* geoff
fi

rm logs/*
rm -Rf neo4jDB
rm -Rf hyperGraphDB
echo "DROP DATABASE edges_development" | mysql -u root
echo "DROP DATABASE flockdb_development" | mysql -u root

./startFlockDB.sh