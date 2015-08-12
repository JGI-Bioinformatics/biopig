

--
-- generates kmer statistics from a fasta file
--

register /.../biopig-core-1.0.0-job.jar
A = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: chararray);
B = foreach A generate id, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 30)) as (kmer:bytearray);
C = group B by kmer;
D = foreach C generate group, COUNT(B);
store D into '$output';
