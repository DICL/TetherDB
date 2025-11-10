#!/bin/bash

echo "start server"
ssh eternitystorage 'nohup /lemma2/rocksdb_storage/greeter_server 1>/dev/null 2>/dev/null &'
sleep 60
echo "server started"
