
--
-- pig script to find all duplicates of a set of sequences within specified edit distance
--
-- reads  = the target set of read sequences (hdfs file path)
-- edit   = the edit distance (int)
-- output = the location for the output (hdfs file path)

register biopig/target/biopig-core-0.2.0-job.jar

%default reads '/scratch/karan/1M.fas'
%default output '/tmp/depreplicate-out'
%default edit 1

define PAIRMERGE gov.jgi.meta.pig.eval.SequencePairMerge();
define CONSENSUS gov.jgi.meta.pig.eval.GenerateConsensus();
define IDENTITYHASH gov.jgi.meta.pig.eval.IdentityHash();
define UNPACK gov.jgi.meta.pig.eval.UnpackSequence();
define EDITDISTANCE gov.jgi.meta.pig.eval.SequenceEditDistance();

-- load the target sequences
READS = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray, header: chararray);

--
-- group the read pairs together by id and filter out any reads that don't have a matching pair.
-- then combine the mate pairs into a single sequence
--
GROUPEDREADS  = group READS by id;
FILTEREDREADS = filter GROUPEDREADS by (COUNT(READS) == 2);
MERGEDREADS   = foreach GROUPEDREADS generate FLATTEN(PAIRMERGE(READS)) as (id: chararray, d: int, seq: bytearray);

-- generate the hash
HASH = foreach MERGEDREADS generate '1', FLATTEN(EDITDISTANCE(IDENTITYHASH(UNPACK(seq)), $edit)), UNPACK(seq);

-- now merge all similar reads together
E = group HASH by $1;
F = foreach E generate $0, COUNT($1), CONSENSUS($1);

-- return output
store F into '$output';

