--
-- given an input file, generate a sorted set of kmers that exist in the dataset.
--
-- similar to the kmerStats.pig script, except this does not keep track of the counts
-- this is useful for alex's kmer-based sequence filtering
--
-- commandline parameters
--   READS=                 the location of the datafile of fastas to read.  for performance,
--                          use bzip to compress the file.  the script works with both bziped and
--                          uncompressed files equally
--   OUTPUTDIR=             the directory to put the results
--   P=                     the level of parallelism for the reduce, defaults to 10

register /.../biopig-core-1.0.0-job.jar

%default READS '/.../1M.fas'
%default P '10'
%default OUTPUTDIR '/.../x'

A = load '$READS' using gov.jgi.meta.pig.storage.FastaStorage as (readid: chararray, d: int, seq: chararray);
B = foreach A generate FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 40)) as (kmer:chararray);
C = DISTINCT B PARALLEL $P;

store C into '$OUTPUTDIR';
