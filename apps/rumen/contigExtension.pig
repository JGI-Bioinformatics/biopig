-- contigextension.pig
--
-- pig script to extend a set of contigs by matching kmers on a set of reads.
-- the matched reads get assembled into the contig.
--
-- parameters:
--    contigs: the set of contigs
--    reads: the set of reads to assemble
--    edit: hamming distance for match to kmer
--    k: kmer size for indexing
--    p: degree of parallelization

register /global/homes/k/kbhatia/local/biopig/lib/biopig-core-0.3.0-job.jar;

%default reads '/users/kbhatia/cloud/HiSeq_100M.fas'
%default output '/users/kbhatia/contigx-out'
%default edit 0
%default k 20
%default p 100

define CONTIGEXTEND gov.jgi.meta.pig.aggregate.ExtendContigWithCap3();

-- load the target sequences
readindex = load '/users/kbhatia/cloud/HiSeq_5000M.fas-generateIndex.pig.out' using PigStorage as (seq: chararray, kmer: bytearray);


-- now index the contigs
contigs = load '/users/kbhatia/cloud/HiSeq_100000M.fas-generateContigs.pig.out' using PigStorage as (geneid: chararray, seq: chararray);
contigindex = foreach contigs generate
              geneid,
              FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20, 0, 10, 1)) as (kmer:bytearray);

-- join reads with the contigs database
j = join readindex by kmer,
         contigindex by kmer PARALLEL $p;
k = foreach j generate
         contigindex::geneid as contigid,
         readindex::seq as readseq;
kk = distinct k PARALLEL $p;
l = group kk by contigid PARALLEL $p;
m = foreach l {
         a = $1.$1;
         generate $0, a;
}

-- join the contigid back with the contigs
n = join contigs by geneid, m by $0 PARALLEL $p;

-- n: {contigs::geneid: chararray,contigs::seq: chararray,m::group: chararray,m::a: {readseq: bytearray}}


-- now assemble
complete = foreach n generate $0, gov.jgi.meta.pig.aggregate.ExtendContigWithCap3($1, $3);
final = filter complete by $1 is not null;

dump final;
