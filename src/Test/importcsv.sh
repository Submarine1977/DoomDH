#!/bin/sh

if [ $# != 1 ] ; then 
echo "USAGE: $0 csvfile" 
echo " e.g.: $0 test.csv" 
exit 1; 
fi 

echo ".connect 127.0.0.1 8000"           > command.txt
echo ".createdatabase poi"               >>command.txt
echo ".setdatabase poi"                  >>command.txt
echo "CREATE TABLE DH_POI(Id INTEGER, SupplierId  INTEGER,  Name   VARCHAR(320),  TransName  VARCHAR(320));" >> command.txt
echo ".importcsv $1 DH_POI"                        >>command.txt
echo "CREATE INDEX dh_poi_id on DH_POI(id);"       >>command.txt
echo "SELECT count() from DH_POI;"                 >>command.txt
echo ".quit"                                       >>command.txt

#oldpath=`pwd`
newpath=`dirname $0`
#echo $newpath
#echo $oldpath

$newpath/../../build/bin/DoomDHShell/ddhshell < command.txt
