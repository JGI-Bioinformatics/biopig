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
 
READS = foreach A generate FLATTEN($1);

B     = foreach A generate FLATTEN( gov.jgi.meta.pig.aggregate.BLAST($1, '/users/kbhatia/data/EC3.2.1.4.faa')); describe B;
-- B: {blastmatches::geneid: chararray,blastmatches::setofsequences: {tuple: (id: chararray,d: int,seq: chararray)}}

C     = foreach B generate $0, FLATTEN($1.$0.id); describe C;
-- C: {blastmatches::geneid: chararray,id::id: chararray}

D     = cogroup C by $1 INNER,  READS by id INNER PARALLEL 100; describe D;
-- D: {group: chararray,C: {blastmatches::geneid: chararray,id::id: chararray},READS: {b::id: chararray,b::d: int,b::sequence: chararray}}

E     = foreach D generate FLATTEN(C.blastmatches::geneid), FLATTEN(READS); describe E;
-- E: {blastmatches::geneid::blastmatches::geneid: chararray,READS::b::id: chararray,READS::b::d: int,READS::b::sequence: chararray}

F     = group E by $0 PARALLEL 100; describe F;
-- F: {group: chararray,E: {blastmatches::geneid::blastmatches::geneid: chararray,READS::b::id: chararray,READS::b::d: int,READS::b::sequence: chararray}}

--
-- now use velvet to build the contigs for each gene-based grouping
--
G     = foreach F generate group, COUNT($1); describe G;
-- G: {contigs: {tuple: (seq: chararray)}}

H     = foreach F generate group, gov.jgi.meta.pig.aggregate.VELVET($1, 1, $0); describe H;

store H into '$output';
