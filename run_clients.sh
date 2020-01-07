#!/bin/bash

counter=1
while [ $counter -le $1 ]
do
go run cmd/client/main.go $counter &
((counter++))
done