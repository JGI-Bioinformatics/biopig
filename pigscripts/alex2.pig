
--
-- step one of the rumen pipeline
--
-- given a file of sequence reads, will blast against cazy, then assemble using cap3
--

register pipelineLib/target/pipelinelibrary-1.0-beta2-job.jar;

--
-- first load the sequence file
--
A = load '$reads' using gov.jgi.meta.pig.storage.FastaBlockStorage
    as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});

B = foreach A generate FLATTEN( gov.jgi.meta.pig.aggregate.kmerMatch($1, '$data', 21));