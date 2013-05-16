#!/usr/bin/python
from org.apache.pig.scripting import *

indexFile = '/users/nordberg/cloud/HiSeq_100000M.fas-generateIndexPacked.pig.out'
contigFile = '/users/nordberg/cloud/HiSeq_100000M.fas-generateIndexPacked.pig.out.txt'
p = 100
Pig.fs("rmr output")

PP = Pig.compile("""
register /global/homes/n/nordberg/local/biopig/lib/biopig-core-1.0.0-job.jar;
-- load the target sequences
        readindex = load '$indexFile' using PigStorage as (seq: bytearray, kmer: bytearray);
-- now index the contigs
        contigs = load '$contigFile' using PigStorage as (geneid: chararray, seq: chararray);

        P = group contigs by $1 PARALLEL $p;

        contigs = foreach P {
            values= $1.$0;
            onlyone = limit values 1;
            generate FLATTEN(onlyone) as geneid, $0 as seq;
        }

        contigindex = foreach contigs generate geneid, FLATTEN(gov.jgi.meta.pig.eval.KmerGenerator(seq, 20)) as (kmer:bytearray);
        j = join readindex by kmer, contigindex by kmer PARALLEL $p;
        k = foreach j generate contigindex::geneid as contigid, gov.jgi.meta.pig.eval.UnpackSequence(readindex::seq) as readseq;
        kk = distinct k PARALLEL $p;
        l = group kk by contigid PARALLEL $p;
        m = foreach l {
                 a = $1.$1;
                 generate $0, a;
        }
        -- join the contigid back with the contigs
        n = join contigs by geneid, m by $0 PARALLEL $p;

        -- now assemble
        contigs = foreach n generate $0 as geneid, gov.jgi.meta.pig.aggregate.ExtendContigWithCap3($1, $3) as res:(seq:chararray, val:int);
        split contigs into notextended if res.val==0, contigs if res.val==1;
        contigs = foreach contigs generate $0 as geneid, res.seq as seq;
        store contigs into 'output/step-$i';
        store notextended into 'output/data-$i';
""")

for i in range(50):
   stats = PP.bind().runSingle()
   if not stats.isSuccessful():
       break;
   if ( stats.getNumberRecords('output/step-'+str(i)) <= 0):
       break;
   else:
       contigFile = 'output/step-'+str(i)
