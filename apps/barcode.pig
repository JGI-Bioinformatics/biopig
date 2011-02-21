--
-- bins a set of sequences based on barcode (first 10 bases)
--
-- given a set of sequences that are barcoded, groups the sequence ids based
-- on the barcode within specified edit distance
--
-- parameters:
--   barcodes - a fasta file of barcodes
--   reads    - a fasta file of reads to bin
--   output   - hdfs directory to put results
-- output schema:
--   (barcodeid, numberofsequences, { reads that match barcode })
--
-- to run type something like:
-- % pig --param barcodes=test/barcodes.fa --param reads=/test/1M.fas --param output=/test/output barcode.pig
--


-- set the default parallelization appropriate for your cluster and data size.
%default p '50'

register biopig-core-0.2.0-job.jar

-- first load the barcodes
a = load 'test/barcodes.fa' using
         gov.jgi.meta.pig.storage.FastaStorage as
         (id: chararray, d: int, seq: bytearray);
b = foreach a generate
         id,
         gov.jgi.meta.pig.eval.UnpackSequence(seq);
c = foreach b generate
         id,
         FLATTEN(gov.jgi.meta.pig.eval.SequenceEditDistance($1, 2));

-- now the sequences
z = load 'test/1M.fas' using
         gov.jgi.meta.pig.storage.FastaStorage as
         (id: chararray, d: int, seq: bytearray);
y = foreach z generate
         id, gov.jgi.meta.pig.eval.SubSequence(seq, 0, 10);
x = foreach y generate
         id,
         gov.jgi.meta.pig.eval.UnpackSequence($1);

-- do a join and clean up outputs
w = join x by $1,
         c by $1 USING 'replicated' PARALLEL $p;
v = foreach w generate
         c::id,
         x::id;
u = distinct v PARALLEL $p;
t = group v by $0;
s = foreach t generate
         group,
         COUNT($1),
         $1;

-- store output
store u into '$output';


