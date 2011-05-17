

--
-- generates kmer statistics from a fasta file
--

register pipelineLib/target/pipelinelibrary-1.0-beta2-job.jar

A = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: chararray);
B = foreach A generate id, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, $k)) as (kmer:chararray);
C = group B by kmer;

store C into '/tmp/xx';

D = foreach C generate group, COUNT(B);
E = group D by $1;
F = foreach E generate group, COUNT(D);

store F into '/tmp/x';
