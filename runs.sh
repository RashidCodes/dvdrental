#!/bin/bash

docker build . -t kingmoh/dvdrental:1.1   
docker run --rm --env-file .env --name dvdrental kingmoh/dvdrental:1.1       
