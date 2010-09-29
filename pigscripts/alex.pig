--
-- filters a set of reads based on shared kmer
--
-- this is for alex's use.  given a set of reads and a set data (both sequence files), it filters
-- the data such that all sequences that pass the filter have at least 1 kmer shared with the
-- reads.  we asssume that size of reads >> size of data.
--
-- commandline parameters include:
--    reads        - the reads
--    data         - the datafile
--    outputdir    - the directory to put output results
--    p            - degree of parallelism to use for reduce operations

%default p '50'

register pipelineLib/target/pipelinelibrary-1.0-beta2-job.jar

A = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (readid: chararray, d: int, seq: chararray);
B = foreach A generate FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 40)) as (kmer:chararray);
C = distinct B PARALLEL $p;

-- use the kmerGenerator to generate the kmers for the reads prior to running this script.
-- then load the grouped kmers directly.
-- C = load '$reads' using BinStorage();

R = load '$data' using gov.jgi.meta.pig.storage.FastaStorage as (dataid: chararray, d: int, seq: chararray);
S = foreach R generate dataid, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 40)) as (kmer:chararray);
T = group S by kmer PARALLEL $p;

-- join B with T
U = join C by kmer, T by group;

-- schema for U is
-- U: {C::group: chararray,C::B: {readid: chararray,kmer: chararray},
--     T::group: chararray,T::S: {dataid: chararray,kmer: chararray}}

V = foreach U generate FLATTEN(T::S.dataid);

W = distinct V PARALLEL $p;

store W into '/user/kbhatia/y';
