--
-- bins a set of sequences based on barcode (first 10 bases)
--
-- given a set of sequences that are barcoded, groups the sequence ids based
-- on the barcode.  supports edit-distance of 2.
--

%default p '10'

register /global/homes/k/kbhatia/local/biopig/lib/biopig-core-0.2.0-job.jar;

-- first load the sequences
a = load '/users/kbhatia/data/barcodes.fa' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray);
b = foreach a generate id, gov.jgi.meta.pig.eval.UnpackSequence(seq);
c = foreach b generate id, FLATTEN(gov.jgi.meta.pig.eval.SequenceEditDistance($1, 1)) as modifiedseq;

-- do a join
w = join b by $1 RIGHT, c by $1 PARALLEL $p;
u = group w by c::id PARALLEL $p;
t = foreach u {
	a = $1.$0;
	b = distinct a;
	c = $1.$3;
	d = distinct c;
	generate group, d, COUNT(b);
};
tt = filter t by $2 <= 1;
s1 = foreach tt generate $0 as id, FLATTEN($1.$0);

-- 2d iteration

c2 = foreach s1 generate id, FLATTEN(gov.jgi.meta.pig.eval.SequenceEditDistance($1, 1)) as modifiedseq;
w2 = join b by $1 RIGHT, c2 by $1 PARALLEL $p;
u2 = group w2 by c2::id PARALLEL $p;
t2 = foreach u2 {
	a = $1.$0;
	b = distinct a;
   	c = $1.$3;	
	d = distinct c; 
	generate group, d, COUNT(b);
};
tt2 = filter t2 by $2 <= 1;
s2 = foreach tt2 generate $0 as id, FLATTEN($1.$0);

-- 3d iteration

c3 = foreach s2 generate id, FLATTEN(gov.jgi.meta.pig.eval.SequenceEditDistance($1, 1)) as modifiedseq;
w3 = join b by $1 RIGHT, c3 by $1 PARALLEL $p;
u3 = group w3 by c3::id PARALLEL $p;
t3 = foreach u3 {
        a = $1.$0;
        b = distinct a;
        c = $1.$3;
        d = distinct c;
        generate group, d, COUNT(b);
};
tt3 = filter t3 by $2 <= 1;
s3 = foreach tt3 generate $0 as id, FLATTEN($1.$0);

-- 4th iteration

c4 = foreach s3 generate id, FLATTEN(gov.jgi.meta.pig.eval.SequenceEditDistance($1, 1)) as modifiedseq;
w4 = join b by $1 RIGHT, c4 by $1 PARALLEL $p;
u4 = group w4 by c4::id PARALLEL $p;
t4 = foreach u4 {
        a = $1.$0;
        b = distinct a;
        c = $1.$3;
        d = distinct c;
        generate group, d, COUNT(b);
};
tt4 = filter t4 by $2 <= 1;
s4 = foreach tt4 generate $0 as id, FLATTEN($1.$0);


-- 5th iteration

c5 = foreach s4 generate id, FLATTEN(gov.jgi.meta.pig.eval.SequenceEditDistance($1, 1)) as modifiedseq;
w5 = join b by $1 RIGHT, c5 by $1 PARALLEL $p;
u5 = group w5 by c5::id PARALLEL $p;
t5 = foreach u5 {
        a = $1.$0;
        b = distinct a;
        c = $1.$3;
        d = distinct c;
        generate group, d, COUNT(b);
};
tt5 = filter t5 by $2 <= 1;

final = foreach tt5 generate $0;

dump final;

-- generate histogram
--r = group s by numberofmatches;
--q = foreach r generate group as numberofmatches, COUNT(s) as numberofbarcodes;
--p = order q by numberofmatches ASC;

--dump p;
