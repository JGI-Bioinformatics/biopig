package test.gov.jgi.meta.hadoop.input;

import gov.jgi.meta.hadoop.input.Util;
import gov.jgi.meta.pig.storage.FastaStorage;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * FastaStorage Tester.
 *
 * @author <Authors name>
 * @since <pre>09/22/2010</pre>
 * @version 1.0
 */
public class FastaStorageTest extends TestCase {
   public FastaStorageTest(String name)
   {
      super(name);
   }


   public void setUp() throws Exception
   {
      super.setUp();
   }


   public void tearDown() throws Exception
   {
      super.tearDown();
   }


  public void testFastaStorageLoad() throws IOException
   {

        PigServer ps = new PigServer(ExecType.LOCAL);
        String script = "a = load 'target/test-classes/1M.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: chararray);\n" +
                "b = group a by '1';\n" +
                "c = foreach b generate COUNT(a);";

        Util.registerMultiLineQuery(ps, script);
        Iterator<Tuple> it = ps.openIterator("c");

        assertEquals(Util.createTuple(new Long[] { new Long(10638) }), it.next());
        assertFalse(it.hasNext());
   }


    public void testKmerGenerator() throws IOException
     {

          PigServer ps = new PigServer(ExecType.LOCAL);
          String script = "a = load '/scratch/karan/1M.fas' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: chararray);\n" +
                  "b = foreach a generate gov.jgi.meta.pig.eval.KmerGenerator(seq, 20);\n" +
                  "c = foreach b generate COUNT($0);";

          Util.registerMultiLineQuery(ps, script);
          Iterator<Tuple> it = ps.openIterator("c");

          assertEquals(Util.createTuple(new Long[] { new Long(57) }), it.next());
     }

    public void testFastaStorageCompressedLoad() throws IOException
     {

          PigServer ps2 = new PigServer(ExecType.LOCAL);
          String script = "a = load 'target/test-classes/1M.fas.bz2' using gov.jgi.meta.pig.storage.FastaStorage as (id: chararray, d: int, seq: chararray);\n" +
                  "b = group a by '1';\n" +
                  "c = foreach b generate COUNT(a);";

          Util.registerMultiLineQuery(ps2, script);
          Iterator<Tuple> it = ps2.openIterator("c");

          assertEquals(Util.createTuple(new Long[] { new Long(10638) }), it.next());
          assertFalse(it.hasNext());
     }


   public static Test suite()
   {
      return(new TestSuite(FastaStorageTest.class ));
   }
}
