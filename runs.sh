#!/bin/bash

# build the image
docker build . -t kingmoh/dvdrental:1.1   

# run the container
docker run --rm --env-file .env --name dvdrental kingmoh/dvdrental:1.1       
