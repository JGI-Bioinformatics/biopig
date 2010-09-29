package gov.jgi.meta.hadoop.input;

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
 * @author <Authors name>
 * @since <pre>09/23/2010</pre>
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




    public static Test suite() {
        return new TestSuite(FastaBlockLineReaderTest.class);
    }
}
