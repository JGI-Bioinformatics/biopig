--   generates the index for nt used by screening

%default p '300'

register /global/homes/k/kbhatia/pipelinelibrary-0.1.1-job.jar

Z = load '/users/kbhatia/SAG_Screening/nt.fas' using gov.jgi.meta.pig.storage.FastaStorage as (dataid: chararray, d: int, seq: bytearray, header: chararray);
Y = filter Z by (NOT (header matches '.*Escherichia.*'));
X = foreach Y generate dataid, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 30)) as (kmer:bytearray);
W = distinct X PARALLEL $p;

store W into '/users/kbhatia/SAG_Screening/ntindex';

