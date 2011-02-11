/*
 * Copyright (c) 2011, The Regents of the University of California, through Lawrence Berkeley
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

package test.gov.jgi.meta.pig.aggregate;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import test.gov.jgi.meta.Util;

import java.util.Iterator;

/**
 * VELVET Tester.
 *
 * @author <Authors name>
 * @since <pre>02/04/2011</pre>
 * @version 1.0
 */
public class VELVETTest extends TestCase {
    public VELVETTest(String name) {
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
     * Method: exec(Tuple input)
     *
     */
    public void testExecBasic() throws Exception {

      PigServer ps = new PigServer(ExecType.LOCAL);
      String script = "A = load 'target/test-classes/1M.fas' using gov.jgi.meta.pig.storage.FastaBlockStorage\n" +
              "    as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});" +
              "C = foreach A generate FLATTEN(gov.jgi.meta.pig.aggregate.VELVET($1, 1, '0'));\n";

      Util.registerMultiLineQuery(ps, script);
      Iterator<Tuple> it = ps.openIterator("C");

      assertEquals(Util.createTuple(new String[]{new String("caagaagcaaaccactcacgaagggagaaaacattatgacagtagcaaacattatttcaatcgtagcagcttgtgagacaatcgatattaagagagcaacgtcacttatcaacctacgttgacgttaatgg")}), it.next());
    }

    public void testExecWithBlast() throws Exception {

      PigServer ps = new PigServer(ExecType.LOCAL);
      String script = "A = load 'target/test-classes/1M.fas' using gov.jgi.meta.pig.storage.FastaBlockStorage\n" +
              "    as (offset: int, b:bag {s:(id: chararray, d: int, sequence: chararray)});" +
              "B = foreach A generate FLATTEN( gov.jgi.meta.pig.aggregate.BLAST($1, '/scratch/karan/EC3.2.1.4.faa'));" +
              "C = foreach B generate FLATTEN(gov.jgi.meta.pig.aggregate.VELVET($1, 1, '0'));\n";

      Util.registerMultiLineQuery(ps, script);
      Iterator<Tuple> it = ps.openIterator("C");

      assertEquals(Util.createTuple(new String[]{new String("caagaagcaaaccactcacgaagggagaaaacattatgacagtagcaaacattatttcaatcgtagcagcttgtgagacaatcgatattaagagagcaacgtcacttatcaacctacgttgacgttaatgg")}), it.next());
    }

    /**
     *
     * Method: arePairedSequences(Tuple s1, Tuple s2)
     *
     */
    public void testArePairedSequences() throws Exception {
        //TODO: Test goes here...
    }



    public static Test suite() {
        return new TestSuite(VELVETTest.class);
    }
}
