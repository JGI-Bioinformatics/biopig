
--
-- pig script to find all duplicates of a set of sequences within specified edit distance
--
-- reads  = the target set of read sequences (hdfs file path)
-- edit   = the edit distance (int)
-- output = the location for the output (hdfs file path)

register /global/homes/k/kbhatia/local/biopig/lib/biopig-core-0.2.0-job.jar;

%default reads '/users/kbhatia/cloud/HiSeq_100M.fas'
%default output '/users/kbhatia/dereplicate-out'
%default edit 0 
%default p 100

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
GROUPEDREADS  = group READS by id PARALLEL $p;
MERGEDREADS   = foreach GROUPEDREADS generate FLATTEN(PAIRMERGE(READS)) as (id: chararray, d: int, seq: bytearray);

-- generate the hash
HASH = foreach MERGEDREADS generate IDENTITYHASH(UNPACK(seq)) as hash, UNPACK(seq) as seq;
HASHNEIGHBORS = foreach HASH generate FLATTEN(EDITDISTANCE($0, $edit)) as hash, '0', $1 as seq;

-- now merge all similar reads together
E = group HASHNEIGHBORS by $0 PARALLEL $p;
F = foreach E generate $0, COUNT($1), CONSENSUS($1);

-- return output
store E into '$output';

