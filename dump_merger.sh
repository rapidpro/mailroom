#!/bin/bash

#MAILROOM_TAG=v7.1.22 # uncomment this line if you want to use a specific tag from nyaruka and set it as desired
rm -f mailroom_test.dump

sleep 1

echo "add remote nyaruka"
git remote add nyaruka https://github.com/nyaruka/mailroom.git

if [ -z $MAILROOM_TAG ]
then 
  echo "checkout mailroom_test.dump from nyaruka main"
  git checkout nyaruka/main -- mailroom_test.dump
else 
  echo "fetch nyaruka remote"
  git fetch nyaruka
  echo "checkout mailroom_test.dump from tag $MAILROOM_TAG"
  sleep 3
  git checkout tags/${MAILROOM_TAG} -- mailroom_test.dump
fi

echo "creating postgres/postgis container"
docker run --name dbdump -d -e POSTGRES_PASSWORD=temba -e PGPASSWORD=temba -p 5432:5432 'postgis/postgis:13-3.1'
sleep 4
echo "setup pg user and db"
docker exec -i dbdump bash -c "PGPASSWORD=temba psql -U postgres --no-password -c \"CREATE USER mailroom_test PASSWORD 'temba';\""
docker exec -i dbdump bash -c "PGPASSWORD=temba psql -U postgres --no-password -c \"ALTER ROLE mailroom_test WITH SUPERUSER;\""
sleep 1
docker exec -i dbdump bash -c "PGPASSWORD=temba psql -U postgres --no-password -c \"CREATE DATABASE mailroom_test;\""
sleep 2
echo "restore dump"
docker exec -i dbdump bash -c "PGPASSWORD=temba pg_restore -v -d mailroom_test -U postgres"  < ./mailroom_test.dump
rm ./mailroom_test.dump
echo "execute sql to add on dump"
sleep 2
cat ./weni_dump.sql | docker exec -i dbdump bash -c "PGPASSWORD=temba psql -U postgres -d mailroom_test"
echo "generate dump"
docker exec -i dbdump bash -c "PGPASSWORD=temba pg_dump -v -U postgres -d mailroom_test -Fc" > ./mailroom_test.dump
docker stop dbdump
docker rm dbdump
