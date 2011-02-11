-- generateContigs.pig
--
-- pig script to generate a set of contigs based on similarity to given gene library
-- parameters:
--    reads: the set of reads to assemble
--    genelib: the gene library
--
-- 2/9/2011 created

register pipelineLib/target/pipelinelibrary-1.0-beta2-job.jar;

--
-- first step: find all the reads that have similarity to the gene library
-- we use tblastn to do the comparison and group results by geneid
-- and then rejoin with the original reads in order to get the mate-pair
--
A     = load '$reads' using gov.jgi.meta.pig.storage.FastaBlockStorage as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});
READS = foreach A generate FLATTEN($1);
B     = foreach A generate FLATTEN( gov.jgi.meta.pig.aggregate.BLAST($1, '$genelib')) as (geneid: chararray, b: bag {s:(id: chararray, direction: int, sequence: chararray)});
C     = foreach B generate $0, FLATTEN($1.$0.id);
-- C is of the form <geneid, readid>
D     = cogroup C by $1, READS by id INNER;
E     = foreach D generate C.blastmatches::geneid, READS;
-- E is of the form <geneid, <set of reads>>

--
-- now use velvet to build the contigs for each gene-based grouping
--
F     = foreach E generate gov.jgi.meta.pig.aggregate.VELVET($1, 1, $0);

dump F;