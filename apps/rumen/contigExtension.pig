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

register ../../biopig/target/biopig-core-0.2.0-job.jar;

%default reads '/scratch/karan/1M.fas'
%default output '/tmp/contigx-out'
%default edit 1
%default k 20
%default p 10

define CONTIGEXTEND gov.jgi.meta.pig.aggregate.ExtendContigWithCap3();

-- load the target sequences
reads = load '/scratch/karan/30mb.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray, header: chararray);
readindex = foreach reads generate
            seq,
            FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20)) as (kmer:bytearray);

-- now index the contigs
contigs = load '/scratch/karan/all_enzymes_contigs.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray, header: chararray);
contigindex = foreach contigs generate
              id,
              FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20, 0, 10, 1)) as (kmer:bytearray);

-- join reads with the contigs database
j = join readindex by kmer,
         contigindex by kmer;
k = foreach j generate
         contigindex::id as contigid,
         gov.jgi.meta.pig.eval.UnpackSequence(readindex::seq) as readseq;
kk = distinct k;
l = group kk by contigid;
m = foreach l {
         a = $1.$1;
         generate $0, a;
}

-- join the contigid back with the contigs
n = join contigs by id, m by $0;

-- now assemble
complete = foreach n generate $0, gov.jgi.meta.pig.aggregate.ExtendContigWithCap3($2, $5);
final = filter complete by $1 is not null;

dump j;
