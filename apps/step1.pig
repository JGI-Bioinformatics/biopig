

--
-- step one of the rumen pipeline
--
-- given a file of sequence reads, will blast against cazy, then assemble using cap3
--

register pipelineLib/target/pipelinelibrary-1.0-beta2-job.jar;

--
-- first load the sequence file
--
A = load '/home/kbhatia/hdfs/30mb.fas' using gov.jgi.meta.pig.storage.FastaBlockStorage
    as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});

--
-- now filter by Blast matches to EC3.2.1.4, and group by gene.  need to group because
-- blast run in parallel could produce multiple geneids
--
B = foreach A generate FLATTEN( gov.jgi.meta.pig.aggregate.BLAST($1, '/home/kbhatia/hdfs/EC3.2.1.4.faa'))
    as (geneid: chararray, b: bag {s:(id: chararray, direction: int, sequence: chararray)});

C = group B by geneid;
D = foreach C generate gov.jgi.meta.pig.aggregate.VELVET(B.b, 1, group);

store D in "x";
