/*
 * Copyright (c) 2010, Joint Genome Institute (JGI) United States Department of Energy
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    This product includes software developed by the JGI.
 * 4. Neither the name of the JGI nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY JGI ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL JGI BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package gov.jgi.meta;

import java.io.*;
import java.net.InetAddress;
import java.util.HashMap;

import gov.jgi.meta.hadoop.input.FastaInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.log4j.Logger;
import org.biojava.bio.seq.impl.SimpleSequence;


/**
 * custom counters
 *
 * MALFORMED is the number of ignored reads that failed simple syntax checking
 * WELLFORMED is the number of reads that were read and processed
 *
 */
enum AssemblyCounters {
   NUMBER_OF_SUCCESSFUL_ASSEMBLERCOMMANDS,
   NUMBER_OF_ERROR_ASSEMBLERCOMMANDS,
   NUMBER_OF_MATCHED_READS,
   NUMBER_OF_GROUPS,
   NUMBER_OF_READS
}


/**
 * hadoop application to take the output of the blatHadoopApplication and
 * assemble the reads in each group.
 *
 * The blat output is a grouping of readids per geneid.  That is, something
 * like:
 * <geneid1 (or group id)>\t<readid1>\t<readid2>\t...
 *
 * This application looks up the readids in the read
 * Set the following properties in a file called: blat-conf.xml that should
 * be in the classpath of hadoop.  The following parameters are used:
 *
 * Hadoop Tuning:
 *   mapred.min.split.size
 *   min hdfs block size (affects the number of map tasks)
 *   number of reduce steps
 *
 * Blast execution:
 *   assembly.commandline - the commandline for blast to execute.
 *   assembly.commandpath - the full path to the blast executable (must be accessible
 *                       on all the hadoop nodes)
 *   assembly.tmpdir - a temporary directory in which a per-run temp directory is
 *                  created.
 *   assembly.cleanup - if false, leave the working directories
 *
 */
public class Assembler {

    private static int calculateNumReducers(String file) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filenamePath = new Path(file);
        if (!fs.exists(filenamePath)) {
            throw new IOException("file not found: " + file);
        }

        FSDataInputStream in = fs.open(filenamePath);
        BufferedReader d
                  = new BufferedReader(new InputStreamReader(in));

        String line;
        line = d.readLine();

        HashMap<String, Integer> m = new HashMap<String,Integer>();

        while (line != null) {
            if (line.matches("^>(.*)-(.*)$")) {
                String groupName = line.split("-")[0];
                if (m.containsKey(groupName)) {
                    m.put(groupName, m.get(groupName) + 1);
                } else {
                    m.put(groupName, 1);
                }
            }
            line = d.readLine();
        }
        in.close();

        return m.size();
    }
    /**
     * starts off the hadoop application
     *
     * @param args specify input file cassandra host and kmer size
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.addResource("assembler-conf.xml");  // application configuration properties
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Logger log = Logger.getLogger(Assembler.class);

        /*
        process arguments
         */

        if (otherArgs.length != 2) {
            System.err.println("Usage: assembler <blatFilterOutputFile> <outputdir>");
            System.exit(2);
        }

        int defaultNumReducers = calculateNumReducers(otherArgs[0]);

        conf.setInt("io.file.buffer.size", 1024 * 1024);

        log.info("main() [version " + conf.getStrings("version", "unknown!")[0] + "] starting with following parameters");
        log.info("\tblat results file: " + otherArgs[0]);
        log.info("\tassembly.cleanup : " + conf.getBoolean("assembly.cleanup", true));
        log.info("\tassembly.skipexecution: " + conf.getBoolean("assembly.skipexecution", false));
        log.info("\tblat.numreducers: " + conf.getInt("assembly.numreducers", defaultNumReducers));

        /*
        setup blast configuration parameters
         */

        Job job = new Job(conf, "assembler");
        job.setJarByClass(Assembler.class);
        job.setInputFormatClass(FastaInputFormat.class);
        job.setMapperClass(FastaTokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(conf.getInt("assembly.numreducers", defaultNumReducers));
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}



