#!/bin/bash

# -Dio.file.buffer.size=3145728
mapcapacity=900
reducecapacity=100

#  500 1000 2000 5000 10000 20000
for x in 20000
  do for r in 1 
   do echo "read size=$x, r=$r"
   size=`ls -l ~/global/cloud/HiSeq_${x}M.fas | awk '{print \$5}'` 
   datafile=/users/kbhatia/cloud/HiSeq_${x}M.fas
   datafilesize=size
   echo "datasize = $size"
   echo "mapcapacity = $mapcapacity"
   echo "reducecapacity = $reducecapacity"
   splitsize=$((($datafilesize)/$mapcapacity))
   echo "splitsize = $splitsize"
#   export PIG_OPTS='-Dmapred.job.name=dereplication,'$x',r='$r' -Dio.file.buffer.size=1048576 -Dio.sort.record.percent=.33 -Dmapred.child.java.opts=-Xmx2G -Dio.sort.factor=-Dio.sort.mb=250 -Dfs.inmemory.size.mb=250 -Dmapred.compress.map.output=false -Dmapred.max.split.size='$splitsize
   export PIG_OPTS='-Dmapred.job.name=dereplication,'$x',r='$r' -Dio.file.buffer.size=1048576 -Dmapred.child.java.opts=-Xmx2G -Dmapred.compress.map.output=true  -Dmapred.max.split.size='$splitsize
   echo $PIG_OPTS > ./dereplication_HiSeq_${x}M.${r}.log 
   echo ~/pig-0.8.0/bin/pig -param reads=/users/kbhatia/cloud/HiSeq_${x}M.fas -param p=$reducecapacity -param output=/users/kbhatia/dereplication/out-${x}.${r} dereplicate.pig >> ./dereplication_HiSeq_${x}M.${r}.log

   time ( ~/pig-0.7.0/bin/pig -param reads=/users/kbhatia/cloud/HiSeq_${x}M.fas -param p=$reducecapacity -param output=/users/kbhatia/dereplication/out-${x}.${r} dereplicate.pig >> ./dereplication_HiSeq_${x}M.${r}.log 2>&1 )  
 done 
done

