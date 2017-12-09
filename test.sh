#!/bin/sh

./DoomDHServer/a.out 127.0.0.1 8000 > server.log &
./DoomDHNode/a.out 100 127.0.0.1 9000 9001 > node100.log &
./DoomDHNode/a.out 100 127.0.0.1 9002 9003 > node101.log &
./Test/a.out > test.log &