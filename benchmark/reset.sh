#!/bin/bash

if [[ $1 == "full" ]]
then
	mv geoffDone/* geoff
fi

rm logs/*
rm -Rf neo4jDB
rm -Rf hyperGraphDB
sudo service mysql start
echo "DROP DATABASE edges_development" | mysql -u root
echo "DROP DATABASE flockdb_development" | mysql -u root
sudo service mysql stop
sudo sh -c "rm /var/lib/mysql/ib*"

bzip2 geoff/*.geoff
