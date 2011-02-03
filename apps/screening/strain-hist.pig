


lookup = load '/users/kbhatia/gi2speciesname/' as (gi: int, junk: chararray, speciesname: chararray);

data = load '$reads' as (readid: chararray, dataid: chararray, numhits: int);
data2 = foreach data generate readid, dataid, STRSPLIT(dataid, '\\x7C', 3);

y = join lookup by gi, data2 by (int) $2.$1;
z = foreach y generate data2::readid, data2::dataid, lookup::speciesname;

a = group z by lookup::speciesname;
b = foreach a generate group, COUNT($1);
c = order b by $1 DESC;

store c into '$output' using PigStorage(',');
