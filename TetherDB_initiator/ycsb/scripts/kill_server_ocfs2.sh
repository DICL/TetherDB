#!/bin/bash

echo "kill server"
ssh eternitystorage 'ps -ef | grep greeter' | awk '{print $2}' > pid.tmp
while read line; do
	ssh -n eternitystorage "kill -9 $line &"
done < pid.tmp
sleep 60
echo "server killed"
