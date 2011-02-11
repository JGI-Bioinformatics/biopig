package test.gov.jgi.meta.pig.storage;

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
 * FastaBlockStorage Tester.
 *
 * @author <Authors name>
 * @since <pre>02/04/2011</pre>
 * @version 1.0
 */
public class FastaBlockStorageTest extends TestCase {
    public FastaBlockStorageTest(String name) {
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
     * Method: getNext()
     *
     */

         public void testFastaBlockStorageLoad() throws IOException
   {
                        // /scratch/nt_2011-01-11_shredded_1k.fas
        PigServer ps = new PigServer(ExecType.LOCAL);
        String script = "a = load 'target/test-classes/1M.fas' using gov.jgi.meta.pig.storage.FastaBlockStorage as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});\n" +
                "c = foreach a generate COUNT(b);";

        Util.registerMultiLineQuery(ps, script);
        Iterator<Tuple> it = ps.openIterator("c");

        assertEquals(Util.createTuple(new Long[] { new Long(10638) }), it.next());
        assertFalse(it.hasNext());
   }
    


            public void testFastaBlockStorageReadDetails() throws IOException
   {
                        // /scratch/nt_2011-01-11_shredded_1k.fas
        PigServer ps = new PigServer(ExecType.LOCAL);
        String script = "a = load 'target/test-classes/1M.fas' using gov.jgi.meta.pig.storage.FastaBlockStorage as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});\n" +
                "b = foreach a generate FLATTEN(b.$0.$0);\n" +
                "c = limit b 1;\n";

        Util.registerMultiLineQuery(ps, script);
        Iterator<Tuple> it = ps.openIterator("c");

        assertEquals(Util.createTuple(new String[] { "756:1:1:1597:17929" }), it.next());
        assertFalse(it.hasNext());
   }
    /**
     *
     * Method: getInputFormat()
     *
     */
    public void testStuff() throws Exception {

       PigServer ps = new PigServer(ExecType.LOCAL);
        String script = "A     = load 'target/test-classes/1M.fas' using gov.jgi.meta.pig.storage.FastaBlockStorage as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});\n" +
                "READS = foreach A generate FLATTEN($1);\n" +
                "B     = foreach A generate FLATTEN( gov.jgi.meta.pig.aggregate.BLAST($1, '/scratch/karan/EC3.2.1.4.faa'));\n" +
                "C     = foreach B generate $0 as geneid, FLATTEN($1.$0.id) as readid;\n" +
                "D     = join C by readid, READS by b::id;\n";
                //"E     = foreach D generate C.geneid, READS;\n";

        String script2 = "a = load 'target/test-classes/one.txt' as (v: chararray, w: chararray);\n" +
                "b = load 'target/test-classes/one.txt' as (v: chararray, w: chararray);\n" +
                "c = group a by 1;\n" +
                "c2 = foreach c generate $0 as offset:int, $1 as b:bag {s:(id: chararray, sequence: chararray)};\n" +
                "d = foreach c2 generate FLATTEN($1);" +
                "e = join d by $0, b by v;";

        Util.registerMultiLineQuery(ps, script2);
        Iterator<Tuple> it = ps.openIterator("e");

        assertEquals(Util.createTuple(new String[] { "756:1:1:1597:17929" }), it.next());
        assertFalse(it.hasNext());
       
    }

    /**
     *
     * Method: prepareToRead(RecordReader reader, PigSplit split)
     *
     */
    public void testPrepareToRead() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: setLocation(String location, Job job)
     *
     */
    public void testSetLocation() throws Exception {
        //TODO: Test goes here...
    }



    public static Test suite() {
        return new TestSuite(FastaBlockStorageTest.class);
    }
}
