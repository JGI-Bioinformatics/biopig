-- generateIndex.pig
--
-- parameters:
--    reads: the set of reads to assemble
--    edit: hamming distance for match to kmer
--    k: kmer size for indexing
--    p: degree of parallelization

register /global/homes/n/nordberg/local/biopig/lib/biopig-core-0.3.0-job.jar;

%default reads '/users/kbhatia/cloud/HiSeq_100M.fas'
%default output '/users/kbhatia/contigx-out'
%default edit 0
%default k 20
%default p 100

-- load the target sequences
reads = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray, header: chararray);
readindex = foreach reads generate gov.jgi.meta.pig.eval.UnpackSequence(seq) as seq, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20)) as (kmer:bytearray);

store readindex into '$output';
