

--
-- generates kmer statistics from a fasta file
--

register /global/homes/n/nordberg/local/biopig/lib/biopig-core-0.3.0-job.jar
A = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: chararray);
B = foreach A generate id, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 30)) as (kmer:bytearray);
C = group B by kmer;
D = foreach C generate group, COUNT(B);
E = group D by $1;
F = foreach E generate group, COUNT(D);
store F into '$output';
