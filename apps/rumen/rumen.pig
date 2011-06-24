-- generateContigs.pig
--
-- pig script to generate a set of contigs based on similarity to given gene library
-- parameters:
--    reads: the set of reads to assemble
--    genelib: the gene library
--
-- 2/9/2011 created

--register /global/homes/n/nordberg/local/biopig/lib/biopig-core-0.3.0-job-pig0.8.1.jar;
register /global/homes/n/nordberg/local/biopig/lib/biopig-core-0.3.0-job.jar;

--
-- first step: find all the reads that have similarity to the gene library
-- we use tblastn to do the comparison and group results by geneid
-- and then rejoin with the original reads in order to get the mate-pair
--
A     = load '$reads' using gov.jgi.meta.pig.storage.FastaBlockStorage as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});
 
reads = foreach A generate FLATTEN($1);

B     = foreach A generate FLATTEN( gov.jgi.meta.pig.aggregate.BLAST($1, '/users/kbhatia/data/EC3.2.1.4.faa')); describe B;
-- B: {blastmatches::geneid: chararray,blastmatches::setofsequences: {tuple: (id: chararray,d: int,seq: chararray)}}

C     = foreach B generate $0, FLATTEN($1.$0.id); describe C;
-- C: {blastmatches::geneid: chararray,id::id: chararray}

D     = cogroup C by $1 INNER, reads by id INNER PARALLEL 100; describe D;
-- D: {group: chararray,C: {blastmatches::geneid: chararray,id::id: chararray},READS: {b::id: chararray,b::d: int,b::sequence: chararray}}

E     = foreach D generate FLATTEN(C.blastmatches::geneid), FLATTEN(READS); describe E;
-- E: {blastmatches::geneid::blastmatches::geneid: chararray,READS::b::id: chararray,READS::b::d: int,READS::b::sequence: chararray}

F     = group E by $0 PARALLEL 100; describe F;
-- F: {group: chararray,E: {blastmatches::geneid::blastmatches::geneid: chararray,READS::b::id: chararray,READS::b::d: int,READS::b::sequence: chararray}}

--
-- now use velvet to build the contigs for each gene-based grouping
--
--G     = foreach F generate group, COUNT($1); describe G;
-- G: {contigs: {tuple: (seq: chararray)}}

--H     = foreach F generate group, gov.jgi.meta.pig.aggregate.VELVET($1, 1, $0); describe H;
contigs     = foreach F generate group, gov.jgi.meta.pig.aggregate.VELVET($1, 1, $0) as (id: chararray, seq: bytearray);

--store H into '$output';
--store H into '$output' using gov.jgi.meta.pig.storage.FastaOutput;

-- load the target sequences
--reads = load '/scratch/karan/30mb.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray, header: chararray);
readindex = foreach reads generate seq, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20)) as (kmer:bytearray);

-- now index the contigs
--contigs = load '/scratch/karan/all_enzymes_contigs.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray, header: chararray);
contigindex = foreach contigs generate id, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20, 0, 10, 1)) as (kmer:bytearray);

-- join reads with the contigs database
j = join readindex by kmer, contigindex by kmer;

k = foreach j generate contigindex::id as contigid, gov.jgi.meta.pig.eval.UnpackSequence(readindex::seq) as readseq;

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

--dump n; 
store n into '$output';
