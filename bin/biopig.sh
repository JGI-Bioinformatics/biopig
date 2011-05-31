#!/bin/sh
#
# biopig wrapper script
#
# usage: biopig <num maps> <num reducers> <script> <hdfsinputfiles> [optional: -param x=y -param a=b ...]
# will run the script on each of the input files specified, with any additional parameters
# set as specified.  For each input file, it sets the max block size such that m mappers are created and sets
# the number of reducers to p.

for file in `hadoop fs -ls $4 | awk '{print \$8}'`
do
   echo "****** processing file $file ***********"

   size=`hadoop fs -ls $file | awk '{print \$5}'`
   splitsize=$((($size)/$1))
   outputfile=${file}-${3##*/}.out
   export PIG_OPTS="-Dmapred.job.name=[biopig]:$3:${file##*/} -Dmapred.child.java.opts=-Xmx2G -Dmapred.compress.map.output=true -Dmapred.max.split.size=$splitsize"
   command="~/pig-0.7.0/bin/pig -param reads=$file -param p=$2 -param output=$outputfile $3"

   echo "size           = $size"
   echo "mapcapacity    = $1"
   echo "reducecapacity = $2"
   echo "splitsize      = $splitsize"
   echo "pig_opts       = $PIG_OPTS"
   echo "command        = $command"

   time ( eval $command  >> ./${file##*/}.log 2>&1 )
   tail ./${file##*/}.log

done
