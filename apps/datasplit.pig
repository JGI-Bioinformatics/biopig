

register pipelineLib/target/pipelinelibrary-1.0-beta2-job.jar;

A = load '/scratch/karan/30mb.fas' using gov.jgi.meta.pig.storage.FastaBlockStorage
    as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});

split A into A1 if ($1 > 0 AND $1 <= 3000000), A2 if ($1 >3000000 AND $1 <= 6000000), A3 if ($1 > 6000000);



