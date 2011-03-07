package test.gov.jgi.meta.pig.eval;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import test.gov.jgi.meta.Util;

import java.io.IOException;
import java.util.Iterator;

/**
 * GenerateConsensus Tester.
 *
 * @author <Authors name>
 * @since <pre>02/04/2011</pre>
 * @version 1.0
 */
public class GenerateConsensusTest extends TestCase {
    public GenerateConsensusTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     *
     * Method: exec(Tuple input)
     *
     */

    public void testGeneateConsensus() throws IOException
     {

          PigServer ps = new PigServer(ExecType.LOCAL);
          String script = "a = load 'target/test-classes/1M.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray);\n" +

                  "b = foreach a generate '1', '1', gov.jgi.meta.pig.eval.UnpackSequence(gov.jgi.meta.pig.eval.SubSequence(seq, 0, 10));\n" +
                  "c = group b by $0;" +
                  "d = foreach c generate gov.jgi.meta.pig.eval.GenerateConsensus($1).$2;";

          Util.registerMultiLineQuery(ps, script);
          Iterator<Tuple> it = ps.openIterator("d");

         assertEquals("cgccgcggca", ((String) it.next().get(0)));
     }

    public void testGeneateConsensus2() throws IOException
     {
          PigServer ps = new PigServer(ExecType.LOCAL);
          String script = "a = load 'target/test-classes/one.txt' using PigStorage() as (one: int);\n" +
                  "b = foreach a generate { ( '1', '1', 'aaaaaa'), ('1', '1', 'aaaaaa'), ('1', '1', 'cccccc') };\n" +
                  "d = foreach b generate gov.jgi.meta.pig.eval.GenerateConsensus($0).$2;";

          Util.registerMultiLineQuery(ps, script);
          Iterator<Tuple> it = ps.openIterator("d");

         assertEquals("aaaaaa", ((String) it.next().get(0)));
     }


     public void testGeneateConsensusWithUnevenSeqs() throws IOException
     {
          PigServer ps = new PigServer(ExecType.LOCAL);
          String script = "a = load 'target/test-classes/one.txt' using PigStorage() as (one: int);\n" +
                  "b = foreach a generate { ( '1', '1', 'aaa'), ('1', '1', 'aaaa'), ('1', '1', 'cccccc') };\n" +
                  "d = foreach b generate gov.jgi.meta.pig.eval.GenerateConsensus($0).$2;";

          Util.registerMultiLineQuery(ps, script);
          Iterator<Tuple> it = ps.openIterator("d");

         assertEquals("aaaacc", ((String) it.next().get(0)));
     }

    public void testGeneateConsensusWithNs() throws IOException
    {
         PigServer ps = new PigServer(ExecType.LOCAL);
         String script = "a = load 'target/test-classes/one.txt' using PigStorage() as (one: int);\n" +
                 "b = foreach a generate { ( '1', '1', 'cccccc'), ('1', '1', 'cccncc'), ('1', '1', 'aaaaaa') };\n" +
                 "d = foreach b generate gov.jgi.meta.pig.eval.GenerateConsensus($0).$2;";

         Util.registerMultiLineQuery(ps, script);
         Iterator<Tuple> it = ps.openIterator("d");

        assertEquals("cccacc", ((String) it.next().get(0)));
    }



    /**
     *
     * Method: fastaConsensusSequence(DataBag values)
     *
     */
    public void testFastaConsensusSequence() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: getCounts(String s, int i)
     *
     */
    public void testGetCounts() throws Exception {
        //TODO: Test goes here...
    }



    public static Test suite() {
        return new TestSuite(GenerateConsensusTest.class);
    }
}
