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


-- load the target sequences
readindex = load '/users/nordberg/cloud/HiSeq_10000M.fas-generateIndexPacked.pig.out' using PigStorage as (seq: bytearray, kmer: bytearray);

-- now index the contigs
contigs = load '/users/nordberg/cloud/HiSeq_10000M.fas-generateContigs.kmer.pig.out.txt' using PigStorage as (geneid: chararray, seq: chararray);

iter = (1,2);

foreach iter {

	contigindex = foreach contigs generate geneid, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20)) as (kmer:bytearray);
	
	-- join reads with the contigs database
	j = join readindex by kmer, contigindex by kmer PARALLEL $p;
	
	k = foreach j generate contigindex::geneid as contigid, gov.jgi.meta.pig.eval.UnpackSequence(readindex::seq) as readseq;
	
	kk = distinct k PARALLEL $p;
	l = group kk by contigid PARALLEL $p;
	m = foreach l {
	         a = $1.$1;
	         generate $0, a;
	}
	-- join the contigid back with the contigs
	n = join contigs by geneid, m by $0 PARALLEL $p;
	
	-- store n into '$output.n';
	-- n: {contigs::geneid: chararray,contigs::seq: chararray,m::group: chararray,m::a: {readseq: bytearray}}
	
	-- now assemble
	contigs = foreach n generate $0, gov.jgi.meta.pig.aggregate.ExtendContigWithCap3($1, $3);
	
} b = $0;

complete = contigs;

-- final = filter complete by $0 is not null;
store complete into '$output';
