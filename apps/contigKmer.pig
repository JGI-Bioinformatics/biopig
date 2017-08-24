
--
-- extend contigs based on directed assembly
--

-- first register the biopig libraries and define commands
register pipelineLib/target/pipelinelibrary-1.0-beta2-job.jar;
DEFINE KMERS gov.jgi.meta.pig.eval.KmerGenerator();

-- load contig file
A = load '$contigs' using gov.jgi.meta.pig.storage.FastaStorage as (contigid: chararray, d: int, seq: bytearray);

-- generate kmer index of the contig ends
B = foreach A generate FLATTEN(KMERS(seq)) as (kmer:chararray), contigid;

-- load the reads
R = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (readid: chararray, d: int, seq: bytearray);
S = foreach R generate FLATTEN(KMERS(seq)) as (kmer:chararray), readid;
T = group S by kmer;

-- join B with T
U = cogroup B by kmer INNER, T by $0 INNER;

V = foreach U generate FLATTEN(B.contigid), FLATTEN(T.S.readid);
W = distinct V;
X = group W by $0;

store X into 'y';