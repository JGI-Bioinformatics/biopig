

--
-- generates N50 histogram.
--
-- input params
--   reads - the input sequences
--       p - degree of parallelism
--  output - outputfile
--
--
-- output:
--   a file with each line representing: <seq length> <total number of basepairs>
--

%default p '1'

register pipelineLib/target/pipelinelibrary-0.1.0-job.jar

-- load the reads
reads       = LOAD '$reads' USING gov.jgi.meta.pig.storage.FastaStorage AS (id: chararray, d: int, seq: chararray);

-- foreach read, generate its size (number of bases) and sort set by size
readsizes   = GROUP (SORT (FOREACH reads GENERATE SIZE(seq) AS size) BY size) all;

-- calculate the total number of bases as a sum of the individual counts
readcounts  = FOREACH readsizes GENERATE SUM(readsizes) AS totalcount;

-- finally, stream over sorted sizes and return the one size that goes over the mid way point
n50         = FOREACH readsizes GENERATE N50(readsizes,(long)(readcounts.totalcount/2));

dump n50;
