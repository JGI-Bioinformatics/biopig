

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

register biopig/target/biopig-core-0.2.0-job.jar;

-- load the reads
reads       = LOAD 'test/all_bacteria_20100224.fna' USING gov.jgi.meta.pig.storage.FastaStorage AS (id: chararray, d: int, seq: bytearray, header: chararray);

-- foreach read, generate its size (number of bases) and sort set by size
readsizes   = FOREACH reads GENERATE SIZE(seq) AS size;
sorted      = ORDER readsizes by size DESC;
grouped     = GROUP sorted by '1';

-- calculate the total number of bases as a sum of the individual counts
counts      = FOREACH grouped GENERATE SUM(sorted) AS totalcount;

-- finally, stream over sorted sizes and return the one size that goes over the mid way point
n50         = FOREACH grouped GENERATE gov.jgi.meta.pig.eval.N50(sorted,((long)counts.totalcount)/2);

dump n50;
