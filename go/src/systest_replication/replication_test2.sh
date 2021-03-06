#!/bin/bash

# Assumes GOPATH only has one path for your project
PROJECT_PATH=$GOPATH

# Pick random ports between [10000, 20000)
STORAGE_PORT1=5002
STORAGE_PORT2=$(((RANDOM % 10000) + 10000))
STORAGE_PORT3=$(((RANDOM % 10000) + 10000))
STORAGE_PORT4=$(((RANDOM % 10000) + 10000))
STORAGE_PORT5=$(((RANDOM % 10000) + 10000))
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
cd ${PROJECT_PATH}/src/systest_replication
go build
cd - > /dev/null

# Start rss store1 (master for startup)
${PROJECT_PATH}/src/rssstoreserver/rssstoreserver -N=2 -B=2 -S=1 -p=${STORAGE_PORT1}> /dev/null &
STORAGE_SERVER1_PID=$!

# Start rss store2
${PROJECT_PATH}/src/rssstoreserver/rssstoreserver -p=${STORAGE_PORT2} -m="localhost:${STORAGE_PORT1}" 2> /dev/null &
STORAGE_SERVER2_PID=$!

# Start rss store3
${PROJECT_PATH}/src/rssstoreserver/rssstoreserver -p=${STORAGE_PORT3} -m="localhost:${STORAGE_PORT1}" 2> /dev/null &
STORAGE_SERVER3_PID=$!

# Start rss store4
${PROJECT_PATH}/src/rssstoreserver/rssstoreserver -p=${STORAGE_PORT4} -m="localhost:${STORAGE_PORT1}" 2> /dev/null &
STORAGE_SERVER4_PID=$!

# Start rss store5
${PROJECT_PATH}/src/rssstoreserver/rssstoreserver -p=${STORAGE_PORT5} -m="localhost:${STORAGE_PORT1}" 2> /dev/null &
STORAGE_SERVER5_PID=$!

sleep 5

# Start test
${PROJECT_PATH}/src/systest_replication/systest_replication "localhost:${STORAGE_PORT1}" "${STORAGE_SERVER1_PID}" "localhost:${STORAGE_PORT2}" "${STORAGE_SERVER2_PID}" "localhost:${STORAGE_PORT3}" "${STORAGE_SERVER3_PID}" "localhost:${STORAGE_PORT4}" "${STORAGE_SERVER4_PID}" "localhost:${STORAGE_PORT5}" "${STORAGE_SERVER5_PID}" "1"

# Kill storage server
kill -9 ${STORAGE_SERVER1_PID}
kill -9 ${STORAGE_SERVER2_PID}
kill -9 ${STORAGE_SERVER3_PID}
kill -9 ${STORAGE_SERVER4_PID}
kill -9 ${STORAGE_SERVER5_PID}
wait ${STORAGE_SERVER_PID1} 2> /dev/null
wait ${STORAGE_SERVER_PID2} 2> /dev/null
wait ${STORAGE_SERVER_PID3} 2> /dev/null
wait ${STORAGE_SERVER_PID4} 2> /dev/null
wait ${STORAGE_SERVER_PID5} 2> /dev/null
