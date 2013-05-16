--
-- filters a set of reads based on shared kmer
--
-- Given a set of reads and a set data (both sequence files), it filters
-- the data such that all sequences that pass the filter have at least 1 kmer shared with the
-- reads.  we asssume that size of reads >> size of data.
--
-- commandline parameters include:
--    reads        - the reads
--    data         - the datafile
--    outputdir    - the directory to put output results
--    p            - degree of parallelism to use for reduce operations

%default p '300'

register /../biopig-core-1.0.0-job.jar;

A = load '/.../SAG_Screening/HWIX_A4_CTACTGCTGA.fas' using gov.jgi.meta.pig.storage.FastaStorage as (readid: chararray, d: int, seq: bytearray, header: chararray); 
B = foreach A generate readid, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 30)) as (kmer:bytearray);
C = distinct B PARALLEL $p;

Z = load '/.../SAG_Screening/nt.fas' using gov.jgi.meta.pig.storage.FastaStorage as (dataid: chararray, d: int, seq: bytearray, header: chararray);
Y = filter Z by (NOT (header matches '.*Escherichia.*'));
X = foreach Y generate FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 30)) as (kmer:bytearray);
W = distinct X PARALLEL $p;

-- join B with T
L = join W by (chararray) kmer, C by (chararray) kmer PARALLEL $p;
M = foreach L generate C::readid;
N = distinct M PARALLEL $p;

store N into 'screening-1';


