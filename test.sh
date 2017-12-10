#!/bin/sh

./DoomDH/DoomDHServer/a.out 127.0.0.1 8000 > server.log &
./DoomDH/DoomDHNode/a.out 100 127.0.0.1 9000 9001 > node100.log &
./DoomDH/DoomDHNode/a.out 101 127.0.0.1 9002 9003 > node101.log &
./DoomDH/Test/a.out > test.log &