package test.gov.jgi.meta.hadoop.input;

import gov.jgi.meta.hadoop.input.FastaBlockLineReader;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.hadoop.io.Text;
import org.junit.Assert;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * FastaBlockLineReader Tester.
 *
 * @author karan bhatia
 * @since <pre>09/22/2010</pre>
 * @version 1.0
 */
public class FastaBlockLineReaderTest extends TestCase {
    public FastaBlockLineReaderTest(String name) {
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

       public void testGetKeyMFile () {

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

    public void testReads() {

        int num = 1;
        int last = -1;

        do {
        try {
            FileInputStream fstream = new FileInputStream("target/test-classes/1M.fas");
            FastaBlockLineReader fblr = new FastaBlockLineReader(fstream);

            Text key = new Text();
            Map<String, String> setofreads = new HashMap<String, String>();
            Map<String, String> setofreadsTotal = new HashMap<String, String>();
            int length = (int) (Math.random() * 10000);
            System.out.println("lenght = " + length);

            int total = 0;

            fblr.readLine(key, setofreads, Integer.MAX_VALUE, length);
//            System.out.println("setofreads.size = " + setofreads.size());
            total += setofreads.size();
            //for (String s : setofreads.keySet()) {
//                System.out.println(s);
//            }
            Runtime r = Runtime.getRuntime();
            while (setofreads.size() > 0) {
                setofreadsTotal.putAll(setofreads);
                setofreads.clear();
                fblr.readLine(key, setofreads, Integer.MAX_VALUE, length);
  //              System.out.println("setofreads.size = " + setofreads.size());
                total += setofreads.size();

                r.gc();
            }
            System.out.println("total = " + total);
            System.out.println("heap size = " + r.totalMemory()/ 1048576);

            if (last != -1) {
               if (last != total) {
                   Assert.fail("error!!!, length = " + length + ": last = " + last + " current = " + total);
               }
            }
            last = total;

        } catch (Exception e) {
            Assert.fail(e.toString());
        }
        } while (num-- > 0);
    }
    

    public static Test suite() {
        return new TestSuite(FastaBlockLineReaderTest.class);
    }
}

