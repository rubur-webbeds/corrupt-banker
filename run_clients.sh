#!/bin/bash

counter=1
while [ $counter -le $1 ]
do
go run client.go $counter &
((counter++))
done