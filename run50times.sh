#!/bin/bash
for ((i=1;i<=100;i++));
do
	echo "ROUND $i";
	make project3bf > ./out-50/out-$i.txt;
done