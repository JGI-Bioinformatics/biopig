#!/bin/bash

mapcapacity=1100
reducecapacity=300

#datafile=/users/kbhatia/SAG/1000_bacteria_20100224.fna
datafilesize=73639894

   echo "datasize=$x"
   echo "mapcapacity = $mapcapacity"
   splitsize=$((($datafilesize)/$mapcapacity))
   echo "splitsize = $splitsize"
   export PIG_OPTS='-Dmapred.task.timeout=162000000 -Dmapred.job.name=screening-nt -Dio.file.buffer.size=1048576 -Dio.sort.record.percent=.33 -Dmapred.child.java.opts=-Xmx2G -Dio.sort.factor='$reducecapacity' -Dio.sort.mb=250 -Dfs.inmemory.size.mb=250 -Dmapred.compress.map.output=true  -Dmapred.max.split.size='$splitsize
#export PIG_OPTS='-Dmapred.task.timeout=162000000 -Dmapred.job.name=screeningtest,'$x',r='$r' -Dmapred.child.java.opts=-Xmx2G' 
   echo $PIG_OPTS > ./screening_alex.log 
   time ( ~/pig/bin/pig alex.pig >> ./screening.log 2>&1 )  

