

--
-- generates kmer statistics from a fasta file
--

%default p '100'


register /global/homes/k/kbhatia/local/biopig/lib/biopig-core-0.2.0-job.jar


A     = load '/users/kbhatia/qc-test-hiseq/kmers' using 
	PigStorage() as 
 	(offset: int, firstkmer: chararray, randomkmer: chararray);

B     = group A by offset PARALLEL $p;

C     = foreach B {
		totalcount  = COUNT(A);
		unique      = distinct A.firstkmer;
		uniquecount = COUNT(unique);
		random      = distinct A.randomkmer; 
		randomcount = COUNT(random);
		generate group, ((float)uniquecount)/((float)totalcount), ((float)randomcount)/((float)totalcount);
	};

store C into '/users/kbhatia/qc-test-hiseq/counts';


