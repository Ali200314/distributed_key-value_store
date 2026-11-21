#!/bin/bash

while true; do
  curl -s "localhost:8073/api/values?key=hello" >/dev/null &
  sleep 0.5
done
