-- generateContigs.pig
--
-- pig script to generate a set of contigs based on similarity to given gene library
-- parameters:
--    reads: the set of reads to assemble
--    genelib: the gene library
--
-- 2/9/2011 created

register biopig/target/biopig-core-0.2.0-job.jar;

--
-- first step: find all the reads that have similarity to the gene library
-- we use tblastn to do the comparison and group results by geneid
-- and then rejoin with the original reads in order to get the mate-pair
--
A     = load '/scratch/karan/1M.fas' using gov.jgi.meta.pig.storage.FastaBlockStorage as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});
READS = foreach A generate FLATTEN($1);
B     = foreach A generate FLATTEN( gov.jgi.meta.pig.aggregate.BLAST($1, '/scratch/karan/EC3.2.1.4.faa'));
C     = foreach B generate $0, FLATTEN($1.$0.id);
-- C is of the form <geneid, readid>
D     = cogroup C by $1 INNER, READS by id INNER;
E     = foreach D generate FLATTEN(C.blastmatches::geneid), FLATTEN(READS);
F     = group E by $0;
G     = foreach F generate gov.jgi.meta.pig.aggregate.VELVET($1, 1, $0);


dump G;