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

register /global/homes/n/nordberg/local/biopig/lib/biopig-core-0.3.0-job.jar;

%default reads '/users/nordberg/cloud/HiSeq_100M.fas'
%default output '/users/nordberg/contigx-out'
%default edit 0
%default k 20
%default p 100

define CONTIGEXTEND gov.jgi.meta.pig.aggregate.ExtendContigWithCap3();

-- load the target sequences
readindex = load '/users/nordberg/cloud/rumen_raw.fas-generateIndexPacked.pig.out' using PigStorage as (seq: bytearray, kmer: bytearray);
-- (JJr"{IK2@c@#dK'K+\Z}E2|x|?,JJr"{I?)
-- (JJr"{IK2@c@#dK'K+\Z}E2|x|?,Jr"{IK?)
-- (JJr"{IK2@c@#dK'K+\Z}E2|x|?,ZbRdAA?)
-- (JJr"{IK2@c@#dK'K+\Z}E2|x|?,vxC-qq?)

-- now index the contigs
--/users/nordberg/cloud/rumen_raw.fas-generateContigs.pig.out
contigs = load '/users/nordberg/data/rumen_raw.biopig-contigs.txt' using PigStorage as (geneid: chararray, seq: chararray);
-- (AAA73867.1,(ccccgtattgcgacatagtggtgactgcatatccaactagtatcacatttccgcttgcaaacatgaggctcgttgtggggaagacggcgaccgtcatgccgacagttaagcccgacaaggcccaatatactttgacctggtcttcggacaacacctcggtagccaaggtgtctaatggaaccgttactgcgcgtagcgtaggcattgctaagattaccgccgaggccacatccctcgatggcgtcaaaacggtgtctgcaagttataatgtcatagttgttgatatcctgcggggcgatgtcaatggtgattcatcgattgatattctcgatgtgacggcactggtttccctcatattgaccggaaacagcaatggaatcaacatggacaatgccgatgttaatggagatggcaaggtggctattgatgatctcactttggtgattgatattatcctcggaaagatatgattgacagaacgatttagactttagcacttttcgatatttgagaattgattgaaataatctgcac))

contigindex = foreach contigs generate geneid, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20, 0, 10, 1)) as (kmer:bytearray);
-- (AAA73867.1,|:FU%Y?)
-- (AAA73867.1,}W@ul+?)
-- (AAA73867.1,}v&c/;?)

-- join reads with the contigs database
j = join readindex by kmer, contigindex by kmer PARALLEL $p;

--store j into 'file:///global/scratch/sd/nordberg/biopig/test/contigExtension-j';

k = foreach j generate contigindex::geneid as contigid, gov.jgi.meta.pig.eval.UnpackSequence(readindex::seq) as readseq;

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
--complete = foreach n generate $0, gov.jgi.meta.pig.aggregate.ExtendContigWithCap3($1, $3);

complete = foreach n {
	r = distinct $3;
	rr = limit r 1000;
	generate $0, gov.jgi.meta.pig.aggregate.ExtendContigWithCap3(contigs::geneid, rr);
}


final = filter complete by $1 is not null;

store final into '$output';
