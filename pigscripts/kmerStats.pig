

--
-- generates kmer statistics from a fasta file
--

register /home/kbhatia/local/meta/hadoop/pipelinelibrary-1.0-beta2-job.jar
define GENERATEKMER gov.jgi.meta.pig.eval.SequencePairMerge();
A = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: chararray);
B = foreach A generate id, FLATTEN(gov.jgi.meta.pig.generator.KmerGenerator(seq)) as (kmer:chararray);
C = group B by kmer;
D = foreach C generate group, COUNT(B);
E = group D by $1;
F = foreach E generate group, COUNT(D);
store F into '/users/kbhatia/x';
