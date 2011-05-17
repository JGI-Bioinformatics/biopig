
%default p '300'

register /global/homes/k/kbhatia/local/biopig/lib/biopig-core-0.2.0-job.jar

Z = load '/users/kbhatia/SAG_Screening/nt.fas' using gov.jgi.meta.pig.storage.FastaStorage as (dataid: chararray, d: int, seq: bytearray, header: chararray);
Y = filter Z by (NOT (header matches '.*Escherichia.*'));

store Y into '/users/kbhatia/tutorial/filter-nt';


