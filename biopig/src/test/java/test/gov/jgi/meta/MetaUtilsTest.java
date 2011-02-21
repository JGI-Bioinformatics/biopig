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

package test.gov.jgi.meta;

import gov.jgi.meta.MetaUtils;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;

import java.util.Set;

/**
 * MetaUtils Tester.
 *
 * @author <Authors name>
 * @since <pre>09/27/2010</pre>
 * @version 1.0
 */
public class MetaUtilsTest extends TestCase {
    public MetaUtilsTest(String name) {
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
     * Method: loadConfiguration(Configuration conf, String[] args)
     *
     */
    public void testLoadConfigurationForConfArgs() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: loadConfiguration(Configuration conf, String configurationFileName, String[] args)
     *
     */
    public void testLoadConfigurationForConfConfigurationFileNameArgs() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: printConfiguration(Configuration conf, Logger log, String[] allProperties)
     *
     */
    public void testPrintConfiguration() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: findAllPaths(Path p)
     *
     */
    public void testFindAllPaths() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: countSequences(String contigFileName)
     *
     */
    public void testCountSequences() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: readSequences(String contigFileName)
     *
     */
    public void testReadSequences() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: sequenceToFile(Map<String, String> seqList, String filename)
     *
     */
    public void testSequenceToFile() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: sequenceToLocalFile(Map<String, String> seqList, String tmpFileName)
     *
     */
    public void testSequenceToLocalFile() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: createTempDir(String tmpDir)
     *
     */
    public void testCreateTempDir() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: recursiveDelete(File fileOrDir)
     *
     */
    public void testRecursiveDelete() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: reverseComplement(String s)
     *
     */
    public void testReverseComplement() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: generateAllNeighbors(String start, int distance)
     *
     */
    public void testGenerateAllNeighborsForStartDistance() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: generateAllNeighbors2(String start, int distance)
     *
     */
    public void testGenerateAllNeighbors() throws Exception {

        Set x = MetaUtils.generateAllNeighbors("AAA".toLowerCase(), 1);
        assertEquals(12, x.size());
        x = MetaUtils.generateAllNeighbors("AAAACAGTCT".toLowerCase(), 1);
        assertEquals(41, x.size());
        x = MetaUtils.generateAllNeighbors("AAAACAGTCT".toLowerCase(), 2);
        assertEquals(721, x.size());
        x = MetaUtils.generateAllNeighbors("AAAACAGTCT".toLowerCase(), 3);
        assertEquals(7681, x.size());
        x = MetaUtils.generateAllNeighbors("AAAACAGTCT".toLowerCase(), 4);
        assertEquals(53761, x.size());
    }

    /**
     *
     * Method: generateAllNeighbors(String start, int distance, Set x)
     *
     */
    public void testGenerateAllNeighborsForStartDistanceX() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: stringReplaceIth(String s, int i, char c)
     *
     */
    public void testStringReplaceIth() throws Exception {
        //TODO: Test goes here...
    }

    /**
     *
     * Method: configureLog4j()
     *
     */
    public void testConfigureLog4j() throws Exception {
        //TODO: Test goes here...
    }


    /**
     *
     * Method: generateHammingDistanceOne(String start)
     *
     */
    public void testGenerateHammingDistanceOne() throws Exception {
        //TODO: Test goes here...
        /*
        try {
           Method method = MetaUtils.class.getMethod("generateHammingDistanceOne", String.class);
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
     * Method: generateHammingDistanceTwo(String start)
     *
     */
    public void testGenerateHammingDistanceTwo() throws Exception {
        //TODO: Test goes here...
        /*
        try {
           Method method = MetaUtils.class.getMethod("generateHammingDistanceTwo", String.class);
           method.setAccessible(true);
           method.invoke(<Object>, <Parameters>);
        } catch(NoSuchMethodException e) {
        } catch(IllegalAccessException e) {
        } catch(InvocationTargetException e) {
        }
        */
        }


    public static Test suite() {
        return new TestSuite(MetaUtilsTest.class);
    }
}
