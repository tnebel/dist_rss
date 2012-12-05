#!/bin/bash

# Assumes GOPATH only has one path for your project
PROJECT_PATH=$GOPATH

# Pick random ports between [10000, 20000)
STORAGE_PORT1=5002
STORAGE_PORT2=$(((RANDOM % 10000) + 10000))
STORAGE_PORT3=$(((RANDOM % 10000) + 10000))
APP_PORT=$(((RANDOM % 10000) + 10000))

# Build rss store server
cd ${PROJECT_PATH}/src/rssstoreserver
go build
cd - > /dev/null

# Build master server
cd ${PROJECT_PATH}/src/masterserver
go build
cd - > /dev/null

# Build test binary
cd ${PROJECT_PATH}/src/systest_mult
go build
cd - > /dev/null

# Start rss store1 (master for startup)
${PROJECT_PATH}/src/rssstore -N=3 -B=0 -S=0 -p=${STORAGE_PORT1}> /dev/null &
STORAGE_SERVER1_PID=$!

# Start rss store2
${PROJECT_PATH}/src/rssstore -p=${STORAGE_PORT2} -m="localhost:${STORAGE_PORT1}" 2> /dev/null &
STORAGE_SERVER2_PID=$!

# Start rss store3
${PROJECT_PATH}/src/rssstore -p=${STORAGE_PORT3} -m="localhost:${STORAGE_PORT1}" 2> /dev/null &
STORAGE_SERVER3_PID=$!

sleep 5

# Start test
${PROJECT_PATH}/src/systest/systest_mult "localhost:${STORAGE_PORT1}"

# Kill storage server
kill -9 ${STORAGE_SERVER1_PID}
kill -9 ${STORAGE_SERVER2_PID}
kill -9 ${STORAGE_SERVER3_PID}
wait ${STORAGE_SERVER_PID1} 2> /dev/null
wait ${STORAGE_SERVER_PID2} 2> /dev/null
wait ${STORAGE_SERVER_PID3} 2> /dev/null
