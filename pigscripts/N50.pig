

--
-- generates N50 histogram.
--
-- input params
--   reads - the input sequences
--       p - degree of parallelism
--  output - outputfile
--
--
-- output:
--   a file with each line representing: <seq length> <total number of basepairs>
--

%default p '1'
%default output '/tmp/x'

SET default_parallel $p;

register pipelineLib/target/pipelinelibrary-0.1.0-job.jar 

A = load '$reads' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: chararray);
B = foreach A generate SIZE(seq);
C = group B by $0;
D = foreach C generate group, SUM(B);
--E = order D by $0 DESC;

store D into '$output';
