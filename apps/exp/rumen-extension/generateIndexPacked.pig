-- generateIndexPacked.pig
--
-- parameters:
--    reads: the set of reads to assemble
--    edit: hamming distance for match to kmer
--    k: kmer size for indexing
--    p: degree of parallelization

register /global/homes/n/nordberg/local/biopig/lib/biopig-core-0.3.0-job.jar;
--register /global/homes/n/nordberg/local/biopig/lib/biopig-core-0.3.0-job-pig0.8.1.jar;

%default reads '/users/nordberg/cloud/HiSeq_100M.fas'
%default output '/users/nordberg/contigx-out'
%default edit 0 
%default k 20
%default p 100

-- load the target sequences
reads = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray, header: chararray);

readindex = foreach reads generate seq, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20)) as (kmer:bytearray); 

--store readindex into 'file:///global/scratch/sd/nordberg/biopig/HiSeq_100M.fas-generateIndexPacked.pig.out';
store readindex into '$output';
