-- generateContigs.pig
--
-- pig script to generate a set of contigs based on similarity to given gene library
-- parameters:
--    reads: the set of reads to assemble
--    genelib: the gene library
--
-- 2/9/2011 created

register /global/homes/k/kbhatia/local/biopig/lib/biopig-core-0.2.0-job.jar;

--
-- first step: find all the reads that have similarity to the gene library
-- we use tblastn to do the comparison and group results by geneid
-- and then rejoin with the original reads in order to get the mate-pair
--
A     = load '/global/homes/k/kbhatia/global/data/1M.fas' using gov.jgi.meta.pig.storage.FastaBlockStorage as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});
READS = foreach A generate FLATTEN($1);
B     = foreach A generate FLATTEN( gov.jgi.meta.pig.aggregate.BLAST($1, '/global/homes/k/kbhatia/global/data/EC3.2.1.4.faa'));
C     = foreach B generate $0, FLATTEN($1.$0.id);
D     = cogroup C by $1 INNER, READS by id INNER PARALLEL 10;
E     = foreach D generate FLATTEN(C.blastmatches::geneid), FLATTEN(READS);
F     = group E by $0 PARALLEL 10;
-- E is of the form <geneid, <set of reads>>

--
-- now use velvet to build the contigs for each gene-based grouping
--
G     = foreach F generate gov.jgi.meta.pig.aggregate.VELVET($1, 1, $0);

dump G;
--store F into '/users/kbhatia/rumen-test-generate';
