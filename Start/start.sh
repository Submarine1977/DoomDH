#!/bin/sh

nohup ../build/bin/DoomDHServer/ddhserver 127.0.0.1 8000 &
nohup ../build/bin/DoomDHNode/ddhnode 100 127.0.0.1 9000 9001 &
nohup ../build/bin/DoomDHNode/ddhnode 101 127.0.0.1 9002 9003 &
nohup ../build/bin/DoomDHNode/ddhnode 102 127.0.0.1 9004 9005 &