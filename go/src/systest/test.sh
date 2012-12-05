#!/bin/bash

# Assumes GOPATH only has one path for your project
PROJECT_PATH=$GOPATH

# Pick random ports between [10000, 20000)
STORAGE_PORT=$(((RANDOM % 10000) + 10000))
TRIB_PORT=$(((RANDOM % 10000) + 10000))

# Build rss store server
cd ${PROJECT_PATH}/src/rssstoreserver
go build
cd - > /dev/null

# Build master server
cd ${PROJECT_PATH}/src/masterserver
go build
cd - > /dev/null

# Start rss store
${PROJECT_PATH}/src/rssstore -port=${STORAGE_PORT} 2> /dev/null &
STORAGE_SERVER_PID=$!
sleep 5

# Start tribtest
${PROJECT_PATH}/src/P2-f12/official/tribtest/tribtest -port=${TRIB_PORT} "localhost:${STORAGE_PORT}"

# Kill storage server
kill -9 ${STORAGE_SERVER_PID}
wait ${STORAGE_SERVER_PID} 2> /dev/null
