package test.gov.jgi.meta.exec;

import gov.jgi.meta.exec.BlastCommand;
import gov.jgi.meta.hadoop.input.FastaBlockLineReader;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * BlastCommand Tester.
 *
 * @author <Authors name>
 * @since <pre>09/23/2010</pre>
 * @version 1.0
 */
public class BlastCommandTest extends TestCase {
    public BlastCommandTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testBlastBasic()
   {
      Configuration conf = new Configuration();

      conf.addResource("test-conf.xml");

      /*
       * process arguments
       */

      Map<String, String> l = new HashMap<String, String>();
      Set<String> r = null;

       try {
      Text t       = new Text();
      FileInputStream fstream = new FileInputStream("target/test-classes/1M.fas");
      FastaBlockLineReader in      = new FastaBlockLineReader(fstream);
      int bytes = in.readLine(t, l);

      BlastCommand b = new BlastCommand(conf);
      r = b.exec(l, "target/test-classes/EC3.2.1.4.faa");
      b.cleanup();

       } catch (Exception e) {
           Assert.fail(e.toString());
       }
       // print last 10 lines of output
       Assert.assertTrue(r.size() > 0);
   }

    public void testMakeSureTempFilesAreCleanedUp() {

        File s = null;

        Configuration conf = new Configuration();

        conf.addResource("test-conf.xml");

        try {
            BlastCommand b = new BlastCommand(conf);
            s = b.getTmpDir();
            b.cleanup();
            Assert.assertTrue(!s.exists());
        } catch (Exception e) {
           Assert.fail(e.toString());
        }
    }

    


    public static Test suite() {
        return new TestSuite(BlastCommandTest.class);
    }
}
