#!/bin/bash

# -Dio.file.buffer.size=3145728
mapcapacity=1158
reducecapacity=386
datafile=/users/kbhatia/SAG/all_bacteria_20100224.fna
datafilesize=`ls -l ~/global/data/all_bacteria_20100224.fna | awk '{print \$5}'`

#  500 1000 2000 5000 10000 20000
for x in 10000 20000
  do for r in 1 
   do echo "datasize=$x, r=$r"
   size=`ls -l ~/global/cloud/HiSeq_${x}M.fas | awk '{print \$5}'` 
   datafile=/users/kbhatia/cloud/HiSeq_${x}M.fas
   datafilesize=size
   echo "size = $size"
   echo "datafilesize = $datafilesize"
   echo "mapcapacity = $mapcapacity"
   splitsize=$((($size+$datafilesize)/$mapcapacity))
   echo "splitsize = $splitsize"
#export PIG_OPTS='-Dmapred.task.timeout=162000000 -Dmapred.job.name=screeningtest,'$x',r='$r' -Dio.file.buffer.size=1048576 -Dio.sort.record.percent=.33 -Dmapred.child.java.opts=-Xmx2G -Dio.sort.factor='$reducecapacity' -Dio.sort.mb=250 -Dfs.inmemory.size.mb=250 -Dmapred.compress.map.output=true -Dmapred.max.split.size='$splitsize
   export PIG_OPTS='-Dmapred.task.timeout=162000000 -Dmapred.job.name=screeningtest,'$x',r='$r' -Dmapred.child.java.opts=-Xmx2G -Dmapred.max.split.size='$splitsize
   echo $PIG_OPTS > ./screening_HiSeq_${x}M.${r}.log 
   echo   ~/pig/bin/pig -param k=40 -param reads=/users/kbhatia/cloud/HiSeq_${x}M.fas -param p=$reducecapacity -param data=$datafile -param output=/users/kbhatia/screening/bacteria-${x}.${r} $HOME/git.metagenomics/pigscripts/alex.pig >> ./screening_HiSeq_${x}M.${r}.log

   time ( ~/pig/bin/pig -param k=40 -param reads=/users/kbhatia/cloud/HiSeq_${x}M.fas -param p=$reducecapacity -param data=$datafile -param output=/users/kbhatia/screening/bacteria-${x}.${r} $HOME/git.metagenomics/pigscripts/alex.pig >> ./screening_HiSeq_${x}M.${r}.log 2>&1 )  
 done 
done

