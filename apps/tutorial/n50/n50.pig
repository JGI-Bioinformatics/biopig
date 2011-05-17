%default p '1'

register /global/homes/k/kbhatia/pipelinelibrary-0.1.1-job.jar;

-- load the reads
A = LOAD '/users/kbhatia/data/1M.fas' USING gov.jgi.meta.pig.storage.FastaStorage AS (id: chararray, d: int, seq: bytearray, header: chararray);

-- foreach read, generate its size (number of bases) and sort set by size
Z = foreach A generate SIZE(seq);
Y = ORDER Z by $0 DESC;
B = GROUP Y all;

-- calculate the total number of bases as a sum of the individual counts
readcounts  = foreach B generate SUM(Y);

-- finally, stream over sorted sizes and return the one size that goes over the mid way point
--n50         = FOREACH readsizes GENERATE N50(readsizes,(long)(readcounts.totalcount/2));

dump readcounts;
