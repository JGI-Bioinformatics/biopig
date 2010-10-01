package gov.jgi.meta.hadoop.input;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * FastaBlockRecordReader Tester.
 *
 * @author <Authors name>
 * @since <pre>09/22/2010</pre>
 * @version 1.0
 */
public class FastaBlockRecordReaderTest extends TestCase {

    FastaBlockInputFormat in;


    public FastaBlockRecordReaderTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

        public void testRead1MFile () {


        try {
            FileInputStream fstream = new FileInputStream("target/test-classes/1M.fas");
            FastaBlockLineReader fblr = new FastaBlockLineReader(fstream);

            Text key = new Text();
            Map<String, String> setofreads = new HashMap<String, String>();
            Map<String, String> setofreadsTotal = new HashMap<String, String>();

            fblr.readLine(key, setofreads, Integer.MAX_VALUE, Integer.MAX_VALUE);
            assertEquals(setofreads.size(), 10638);
        } catch (Exception e) {
            System.err.println(e);
            assertTrue(false);
        }
    }

    public static Test suite() {
        return new TestSuite(FastaBlockRecordReaderTest.class);
    }
}
