--
-- given an input file, generate a sorted set of kmers that exist in the dataset.
--
-- similar to the kmerStats.pig script, except this does not keep track of the counts
-- this is useful for alex's kmer-based sequence filtering
--
-- commandline parameters
--   READS=                 the location of the datafile of fastas to read.  for performance,
--                          use bzip to compress the file.  the script works with both bziped and
--                          uncompressed files equally
--   OUTPUTDIR=             the directory to put the results
--   P=                     the level of parallelism for the reduce, defaults to 10

--register /.../biopig-core-1.0.0-job.jar

%default READS '1M.fas'
%default P '10'
%default KMERSIZE '4'
%default OUTPUTDIR 'x'


-- raw data from a fasta file
A = load '../biopig/src/test/resources/1M.fas' using gov.jgi.meta.pig.storage.FastaStorage as (readid: chararray, d: chararray, seq: bytearray, header:chararray);

-- make an identifier for a row in case the readid is not unique
-- B = rank A by readid;
B = foreach A generate CONCAT(readid,header) as uid, seq;

-- save read info for later use
--INFO = foreach B generate $0 as uid, readid, d, header;

-- generate kmers
C0 = foreach B generate $0 as uid, gov.jgi.meta.pig.eval.KmerGenerator(seq, 4) as kmer;
--C0 = foreach B generate $0 as uid, gov.jgi.meta.pig.eval.KmerGenerator(seq, $KMERSIZE) as kmer;
C = foreach C0 generate uid, COUNT(kmer) as count, flatten(kmer) as kmer; 

--unpack the kmers
D = foreach C generate uid, count, gov.jgi.meta.pig.eval.UnpackSequence(kmer) as kmer;

-- consider complement
E = foreach D generate uid, count, gov.jgi.meta.pig.eval.LessKmer(kmer) as kmer;

-- group kmers
F = foreach (group E by (uid, count, kmer)) generate flatten(group), COUNT($1) as cnt;

-- group by uid
G = foreach (group F by (uid, count)) {
	profile=foreach F generate kmer as kmer, cnt as cnt;
	generate group.uid as uid, group.count as count, profile;
	};

-- calculate length
H = foreach G generate uid, count, flatten(gov.jgi.meta.pig.eval.TNFLength(profile)) as length ,profile;

I = foreach H generate uid, count, length, flatten(profile);

-- cnt/length
J = foreach I generate uid, count, length, kmer, 1.0*cnt/length as normlen; 

K = foreach (group J by (uid, count, length) ) {
	profile=foreach J generate kmer,normlen;
	generate flatten(group), profile;
};


store K into '$OUTPUTDIR';
