#!/bin/bash

while true; do
  curl -s "localhost:8071/values/hello" >/dev/null &
  sleep 0.1
done
