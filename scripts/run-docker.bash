#!/bin/bash

#docker run --network network-1121 lh-producer:0.1
docker run --network network-1121 -e HOST_NAME=kafka0 -e PORT_NUM=29092 lh-consumer:0.1

