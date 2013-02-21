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
sudo rm /var/log/upstart/benchmark.log
sudo touch /var/log/upstart/benchmark.log
sudo chmod 666 /var/log/upstart/benchmark.log
bzip2 geoff/*.geoff
