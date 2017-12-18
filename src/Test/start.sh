#!/bin/sh

nohup ../build/bin/DoomDHServer/ddhserver 127.0.0.1 8000 &
nohup ../build/bin/DoomDHNode/ddhnode 100 127.0.0.1 9000 9001 &
nohup ../build/bin/DoomDHNode/ddhnode 101 127.0.0.1 9002 9003 &
nohup ../build/bin/DoomDHNode/ddhnode 102 127.0.0.1 9004 9005 &
echo ".connect 127.0.0.1 8000"           > command.txt
echo ".addchildnode 127.0.0.1 9000 9001" >>command.txt
echo ".addchildnode 127.0.0.1 9002 9003" >>command.txt
echo ".addchildnode 127.0.0.1 9004 9005" >>command.txt
echo ".quit"                             >>command.txt
newpath=`dirname $0`
$newpath/../../build/bin/DoomDHShell/ddhshell < command.txt