#!/bin/bash
go build . &&
(./server -peer "127.0.0.1:3004" -peer "127.0.0.1:3006" -port 3001 -raft 3002 &
./server -peer "127.0.0.1:3002" -peer "127.0.0.1:3006" -port 3003 -raft 3004 &
./server -peer "127.0.0.1:3002" -peer "127.0.0.1:3004" -port 3005 -raft 3006)
