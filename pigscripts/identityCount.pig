
--
-- pig script to count number of sequences of each identity hash and generate
-- a consensues sequence... the beginnings of dereplication.
--

register pipelineLib/target/pipelinelibrary-1.0-beta2.jar
define PAIRMERGE gov.jgi.meta.pig.eval.SequencePairMerge();
define CONSENSUS gov.jgi.meta.pig.eval.GenerateConsensus();

A = load '$fasta' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: chararray);
B = group A by id;
C = foreach B generate flatten(PAIRMERGE(A)) as (id: chararray, d: int, seq: chararray);
D = foreach C generate id, gov.jgi.meta.pig.eval.IdentityHash(seq), seq;
E = group D by $1;
F = foreach E generate $0, COUNT(D), CONSENSUS(D);
dump F;


