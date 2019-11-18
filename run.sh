#!/usr/bin/env bash
docker run -it -u $(id -u):$(id -g) \
               --rm \
               --network="host" \
               -v /$(pwd):/repo \
               -w /repo \
               --name $(basename $("pwd"))\
               $(basename $("pwd")):latest