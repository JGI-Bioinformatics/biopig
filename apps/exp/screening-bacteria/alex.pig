--
-- filters a set of reads based on shared kmer
--
-- this is for alex's use.  given a set of reads and a set data (both sequence files), it filters
-- the data such that all sequences that pass the filter have at least 1 kmer shared with the
-- reads.  we asssume that size of reads >> size of data.
--
-- commandline parameters include:
--    reads        - the reads
--    data         - the datafile
--    outputdir    - the directory to put output results
--    p            - degree of parallelism to use for reduce operations

%default p '300'

register /global/homes/k/kbhatia/pipelinelibrary-0.1.1-job.jar

A = load '/users/kbhatia/SAG_Screening/HWIX_A4_CTACTGCTGA.fas' using gov.jgi.meta.pig.storage.FastaStorage as (readid: chararray, d: int, seq: bytearray, header: chararray); 
B = foreach A generate readid, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 30)) as (kmer:bytearray);
C = distinct B PARALLEL $p;

Z = load '/users/kbhatia/SAG_Screening/nt.fas' using gov.jgi.meta.pig.storage.FastaStorage as (dataid: chararray, d: int, seq: bytearray, header: chararray);
Y = filter Z by (NOT (header matches '.*Escherichia.*'));
X = foreach Y generate dataid, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 30)) as (kmer:bytearray);
W = distinct X PARALLEL $p;

-- join B with T
L = join W by (chararray) kmer, C by (chararray) kmer PARALLEL $p;
M = foreach L generate W::dataid, C::readid;
N = group M by (W::dataid, C::readid) PARALLEL $p;
O = foreach N generate group.C::readid, group.W::dataid, COUNT(M) as numhits;
P = group O by C::readid ;
R = foreach P {
    R1 = order O by numhits DESC ;
    R2 = limit R1 1;
    generate FLATTEN(R2);
};

store R into '/users/kbhatia/screening-3';


