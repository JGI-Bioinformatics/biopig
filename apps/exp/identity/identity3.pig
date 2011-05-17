--
-- bins a set of sequences based on barcode (first 10 bases)
--
-- given a set of sequences that are barcoded, groups the sequence ids based
-- on the barcode.  supports edit-distance of 2.
--

%default p '100'

register /global/homes/k/kbhatia/local/biopig/lib/biopig-core-0.2.0-job.jar;

-- first load the sequences
a = load '/users/kbhatia/data/hairpin-screened.fa' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray);
b = foreach a generate id, gov.jgi.meta.pig.eval.UnpackSequence(seq);
c = foreach b generate id, FLATTEN(gov.jgi.meta.pig.eval.SequenceEditDistance($1, 5));

-- do a join
w = join b by $1, c by $1 PARALLEL $p;
t = group w by c::id PARALLEL $p;
s = foreach t {
	a = order $1 by $0;
	b = a.$0;
	c = distinct b;
	generate FLATTEN(c), COUNT(c), group as xxx, c;
};

s2 = group s by $0 PARALLEL $p;
s3 = foreach s2 {
	a = order $1 by $1 DESC;
	b = limit a 1;
	generate group as node, FLATTEN(b.$2) as rep, FLATTEN(b.$3) as groupname;
};

r = foreach s3 generate rep;

dump r;

--s1 = filter s by numberofmatches == 1;
--store s1 into '/users/kbhatia/hairpin-out';

-- generate histogram
--r = group s by numberofmatches;
--q = foreach r generate group as numberofmatches, COUNT(s) as numberofbarcodes;
--p = order q by numberofmatches ASC;

--dump p;
