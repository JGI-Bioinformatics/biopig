--
-- filters a set of reads based on shared kmer
--

register pipelineLib/target/pipelinelibrary-1.0-beta2-job.jar

A = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (readid: chararray, d: int, seq: chararray);
B = foreach A generate readid, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 40)) as (kmer:chararray);
C = group B by kmer;

R = load '$data' using gov.jgi.meta.pig.storage.FastaStorage as (dataid: chararray, d: int, seq: chararray);
S = foreach R generate dataid, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 40)) as (kmer:chararray);
T = group S by kmer;

-- join B with T
U = join C by B.kmer, T by S.kmer using 'replicated';

-- schema for U is
-- U: {C::group: chararray,C::B: {readid: chararray,kmer: chararray},
--     T::group: chararray,T::S: {dataid: chararray,kmer: chararray}}

V = foreach U generate FLATTEN(T::S.dataid);

W = distinct V;

store W into '/user/kbhatia/y';
