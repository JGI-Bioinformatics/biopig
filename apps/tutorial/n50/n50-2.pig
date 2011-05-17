

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

%default p '300'

register /global/homes/k/kbhatia/biopig-core-0.2.0-job.jar;

-- load the reads
reads       = load '/users/kbhatia/SAG_Screening/nt.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray, header: chararray);

-- foreach read, generate its size (number of bases) and sort set by size
readsizes   = foreach reads generate SIZE(seq) as size;
sorted      = order readsizes by size DESC PARALLEL $p;
grouped     = group sorted by '1' PARALLEL $p;

-- calculate the total number of bases as a sum of the individual counts
counts      = foreach grouped generate SUM(sorted) as totalcount;

-- finally, stream over sorted sizes and return the one size that goes over the mid way point
n50         = foreach grouped generate gov.jgi.meta.pig.eval.N50(sorted,5785080285L);

dump n50; 
