#!/bin/bash
go build . &&
./podclient $(../launch-tool/launch.py client-url 1)
