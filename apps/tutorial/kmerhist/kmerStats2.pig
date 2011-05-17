

--
-- generates kmer statistics from a fasta file
--

%default p '300'

register /global/homes/k/kbhatia/pipelinelibrary-0.1.1-job.jar

A = load '/users/kbhatia/cloud/HiSeq_10000M.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray, header: chararray);
B = foreach A generate FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20)) as (kmer:bytearray);
C = group B by kmer PARALLEL $p;
D = foreach C generate group, COUNT(B);
E = group D by $1 PARALLEL $p;
F = foreach E generate group, COUNT(D);

store F into '/users/kbhatia/tutorial/kmerstats2';
