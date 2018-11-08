#!/bin/bash
go build . &&
./client $(../launch-tool/launch.py client-url 1)
