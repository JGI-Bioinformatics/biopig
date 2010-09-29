package test.gov.jgi.meta.hadoop.reduce;

import static org.mockito.Mockito.*;

import gov.jgi.meta.hadoop.reduce.IdentityReducerGroupByKey;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.util.Arrays;
import java.util.List;

/**
 * IdentityReducerGroupByKey Tester.
 *
 * @author karan bhatia
 * @since <pre>09/28/2010</pre>
 * @version 1.0
 */
public class IdentityReducerGroupByKeyTest extends TestCase {
    public IdentityReducerGroupByKeyTest(String name) {
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
     * Method: reduce(Text key, Iterable<Text> values, Context context)
     *
     */
    public void testReduce() throws Exception {
       IdentityReducerGroupByKey reducer = new IdentityReducerGroupByKey();

       Text key = new Text("abc");
       
       List<Text> values = Arrays.asList(new Text("a"), new Text("b"), new Text("c"));

       Context output = mock(Context.class);

       reducer.reduce(key, values, output);

       verify(output).write(new Text("abc"), new Text("a\tb\tc"));
    }



    public static Test suite() {
        return new TestSuite(IdentityReducerGroupByKeyTest.class);
    }
}
