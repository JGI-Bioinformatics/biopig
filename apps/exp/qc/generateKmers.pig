

--
-- generates kmer statistics from a fasta file
--

%default p '100'
%default N '10'

register /global/homes/k/kbhatia/local/biopig/lib/biopig-core-0.2.0-job.jar


A     = load '/users/kbhatia/data/HiSeq-FCA_s_1.fas' using 
		gov.jgi.meta.pig.storage.FastaBlockStorage as 
		(offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)}); 
READS = foreach A generate 
		offset,
		FLATTEN($1);

B     = foreach READS generate 
		offset,
		FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator($3, 20, 0, 1)) as first,
		FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator($3, 20, -1, 1)) as random;

store B into '/users/kbhatia/qc-test-hiseq/kmers';


