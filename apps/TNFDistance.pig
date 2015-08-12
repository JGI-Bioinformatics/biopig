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

%default READS '1M.fas'
%default P '10'
%default OUTPUTDIR 'x'


A = load '$READS' using gov.jgi.meta.pig.storage.FastaStorage as (readid: chararray, d: chararray, seq: bytearray);

B = foreach A generate readid,gov.jgi.meta.pig.eval.KmerGenerator(seq, 1) as kmers ;

BB = foreach B generate readid, flatten(kmers);

C = foreach BB  generate readid, gov.jgi.meta.pig.eval.UnpackSequence(kmer) as kmer;

D = foreach ( group C by (readid, kmer) ) generate flatten(group), COUNT($1) as cnt;

E = foreach (GROUP D by $0) {

data = foreach $1 generate $1 as kmer, $2 as cnt; 

 generate $0 as readid, data;

};

E2 = foreach E generate *;

J = foreach (CROSS E, E2) generate $0 as read1,$2 as read2, ($1,$3) as data;

L = FOREACH J GENERATE $0 as read1,$1 as read2, flatten(gov.jgi.meta.pig.eval.TNFDistance2(data));



store L into '$OUTPUTDIR';
