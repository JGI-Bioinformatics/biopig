/*
 * Copyright (c) 2010, The Regents of the University of California, through Lawrence Berkeley
 * National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy).
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * (1) Redistributions of source code must retain the above copyright notice, this list of conditions and the
 * following disclaimer.
 *
 * (2) Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 * and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * (3) Neither the name of the University of California, Lawrence Berkeley National Laboratory, U.S. Dept.
 * of Energy, nor the names of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You are under no obligation whatsoever to provide any bug fixes, patches, or upgrades to the
 * features, functionality or performance of the source code ("Enhancements") to anyone; however,
 * if you choose to make your Enhancements available either publicly, or directly to Lawrence Berkeley
 * National Laboratory, without imposing a separate written license agreement for such Enhancements,
 * then you hereby grant the following license: a  non-exclusive, royalty-free perpetual license to install,
 * use, modify, prepare derivative works, incorporate into other computer software, distribute, and
 * sublicense such enhancements or derivative works thereof, in binary and source code form.
 */

package test.gov.jgi.meta.exec;

import gov.jgi.meta.MetaUtils;
import gov.jgi.meta.exec.VelvetCommand;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;

import java.io.File;
import java.util.Map;

/**
 * VelvetCommand Tester.
 *
 * @author <Authors name>
 * @since <pre>09/27/2010</pre>
 * @version 1.0
 */
public class VelvetCommandTest extends TestCase {
    public VelvetCommandTest(String name) {
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
     * Method: exec(String groupId, Map<String, String> seqDatabase, Reducer.Context context)
     *
     */
    public void testExec() throws Exception {

       Configuration conf = new Configuration();
       conf.addResource("test-conf.xml");

     VelvetCommand assemblerCmd = new VelvetCommand(conf);

     Map<String, String> value = MetaUtils.readSequences("target/test-classes/1M.fas");

       try {

          Map<String, String> tmpmap = assemblerCmd.exec("test", value, null);
          assemblerCmd.cleanup();
          Assert.assertTrue(tmpmap.size() == 313);

       } catch (Exception e) {
          Assert.fail(e.toString());
       }


    }

       public void testMakeSureTempFilesAreCleanedUp() {

        File s = null;

        Configuration conf = new Configuration();

        conf.addResource("test-conf.xml");

        try {
            VelvetCommand b = new VelvetCommand(conf);
            s = b.getTmpDir();
            b.cleanup();
            Assert.assertTrue(!s.exists());
        } catch (Exception e) {
           Assert.fail(e.toString());
        }
    }



    public static Test suite() {
        return new TestSuite(VelvetCommandTest.class);
    }
}
