-- strainHist.pig
--
-- generates a histogram of species based on results of the screening
--
-- parameters:
--    input - the output of the screening run
--

%default p '10'

-- first load the gi->species name index
lookup = load '/users/kbhatia/gi2speciesname/' as
       (gi: int, junk: chararray, speciesname: chararray);

-- now load the input data and strip the gi name from the matches
data = load '$input' as
       (readid: chararray, dataid: chararray, numhits: int);
data2 = foreach data generate
       readid,
       dataid,
       STRSPLIT(dataid, '\\x7C', 3);

-- for each dataid, join the speciesname
y = join lookup by gi,
         data2 by (int) $2.$1 PARALLEL $p;
z = foreach y generate
         data2::readid,
         data2::dataid,
         lookup::speciesname;

-- now generate the histogram
a = group z by lookup::speciesname PARALLEL $p;
b = foreach a generate
         group,
         COUNT($1);
c = order b by $1 DESC;

-- write output
store c into '$output' using PigStorage(',');
