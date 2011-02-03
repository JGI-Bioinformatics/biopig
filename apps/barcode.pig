--
-- bins a set of sequences based on barcode (first 10 bases)
--
-- given a set of sequences that are barcoded, groups the sequence ids based
-- on the barcode.  supports edit-distance of 2.
--

%default p '50'

register pipelineLib/target/pipelinelibrary-0.1.1-job.jar;

-- first load the barcodes
a = load 'test/barcodes.fa' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray);
b = foreach a generate id, gov.jgi.meta.pig.eval.UnpackSequence(seq);
c = foreach b generate id, FLATTEN(gov.jgi.meta.pig.eval.SequenceEditDistance($1, 2));

-- now the sequences
z = load 'test/1M.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray);
y = foreach z generate id, gov.jgi.meta.pig.eval.SubSequence(seq, 0, 10);
x = foreach y generate id, gov.jgi.meta.pig.eval.UnpackSequence($1);

-- do a join
w = join c by $1, x by $1;
u = foreach w generate c::id, x::id;

dump u;
