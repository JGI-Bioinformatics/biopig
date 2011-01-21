package test.gov.jgi.meta.sequence;

import gov.jgi.meta.sequence.SequenceString;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * SequenceString Tester.
 *
 * @author <Authors name>
 * @since <pre>12/03/2010</pre>
 * @version 1.0
 */
public class SequenceStringTest extends TestCase {
    public SequenceStringTest(String name) {
        super(name);
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    /**
     *
     * Method: toString()
     *
     */
    @org.junit.Test
    public void testToSequence() throws Exception {

       String s = "attgc";
       String s2 = "attgca";
       String s3 = "ggccaat";
        byte[] b = {(byte) 38, (byte) 237};

       byte[] ss = SequenceString.sequenceToByteArray(s);
       byte[] ss2 = SequenceString.sequenceToByteArray(s2);
       byte[] ss3 = SequenceString.sequenceToByteArray(s3);

       Assert.assertEquals("s: packing != unpacking", s, SequenceString.byteArrayToSequence(ss));
       Assert.assertEquals("s1: packing != unpacking", s2, SequenceString.byteArrayToSequence(ss2));
       Assert.assertEquals("s2: packing != unpacking", s3, SequenceString.byteArrayToSequence(ss3));

       Assert.assertEquals("s: stringify is not what it should be", new String(b), new String(s));

    }

   public void testToString() throws Exception {

      String s = "attgc";
      byte[] b = {(byte) 38, (byte) 237};

      byte[] ss = SequenceString.sequenceToByteArray(s);
      Assert.assertArrayEquals("packing != unpacking", b, ss);

   }






    /**
     *
     * Method: initHash()
     *
     */
    @org.junit.Test
    public void testInitHash() throws Exception {
        //TODO: Test goes here...
        /*
        try {
           Method method = SequenceString.class.getMethod("initHash");
           method.setAccessible(true);
           method.invoke(<Object>, <Parameters>);
        } catch(NoSuchMethodException e) {
        } catch(IllegalAccessException e) {
        } catch(InvocationTargetException e) {
        }
        */
        }

    /**
     *
     * Method: pack(String sequenceToPack)
     *
     */
    @org.junit.Test
    public void testPack() throws Exception {
        //TODO: Test goes here...
        /*
        try {
           Method method = SequenceString.class.getMethod("pack", String.class);
           method.setAccessible(true);
           method.invoke(<Object>, <Parameters>);
        } catch(NoSuchMethodException e) {
        } catch(IllegalAccessException e) {
        } catch(InvocationTargetException e) {
        }
        */
        }


    public static Test suite() {
        return new TestSuite(SequenceStringTest.class);
    }
}
