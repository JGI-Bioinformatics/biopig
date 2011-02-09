package test.gov.jgi.meta.pig.aggregate;

import gov.jgi.meta.exec.BlastCommand;
import gov.jgi.meta.hadoop.input.FastaBlockLineReader;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.hadoop.io.Text;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import test.gov.jgi.meta.Util;

import java.io.FileInputStream;
import java.util.Iterator;

/**
 * BLAST Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>02/04/2011</pre>
 */
public class BLASTTest extends TestCase {
   public BLASTTest(String name) {
      super(name);
   }

   public void setUp() throws Exception {
      super.setUp();
   }

   public void tearDown() throws Exception {
      super.tearDown();
   }

   /**
    * Method: exec(Tuple input)
    */
   public void testExec() throws Exception {

      PigServer ps = new PigServer(ExecType.LOCAL);
      String script = "a = load 'target/test-classes/1M.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: bytearray);\n" +
              "b = foreach a generate id, d, gov.jgi.meta.pig.eval.UnpackSequence(seq);\n" +
              "c = group b by '1';\n" +
              "d = foreach c generate FLATTEN(gov.jgi.meta.pig.aggregate.BLAST(b, 'target/test-classes/EC.faa'));\n" +
              "e = foreach d generate COUNT(blastmatches::setofsequences);\n";


      Util.registerMultiLineQuery(ps, script);
      Iterator<Tuple> it = ps.openIterator("e");

      assertEquals(Util.createTuple(new Long[]{new Long(13)}), it.next());
}


   public static Test suite() {
      return new TestSuite(BLASTTest.class);
   }
}
