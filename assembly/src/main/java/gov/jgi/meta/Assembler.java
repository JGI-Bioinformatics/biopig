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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import gov.jgi.meta.cassandra.DataStore;
import gov.jgi.meta.exec.AssemblerCommand;
import gov.jgi.meta.hadoop.input.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.log4j.Logger;


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

    /**
     * map task reads portions of the fasta file provided from the input split and
     * generated the kmers and inserts them directly into the cassandra datastore.
     */
    public static class LineTokenizerMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private int batchSize = 100;
        private int currentSize = 0;

        Logger log = Logger.getLogger(LineTokenizerMapper.class);

        /**
         * abstracts the details of connections to the cassandra servers
         */
        DataStore ds = null;

        /**
         * blast command wrapper
         */
        AssemblerCommand assemblerCmd = null;

        /**
         * initialization of mapper retrieves connection parameters from context and
         * opens connection to datastore
         *
         * @param context is the mapper context for this job
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        protected void setup(Context context)
                throws IOException, InterruptedException {

            log.debug("initializing map task for job: " + context.getJobName());
            log.debug("initializing maptask on host: " + InetAddress.getLocalHost().getHostName());

            assemblerCmd = new AssemblerCommand(context.getConfiguration());

        }

        /**
         * free resource after mapper has finished, ie close socket to cassandra server
         *
         * @param context is the job context for the map task
         */
        protected void cleanup(Context context) throws IOException {

            log.info("deleting map task for job: " + context.getJobName() + " on host: " +  InetAddress.getLocalHost().getHostName());

            if (ds != null) ds.cleanup();

        }


        /**
         * the map function processes a block of fasta reads through the blast program
         *
         */
        public void map(LongWritable lineNum, Text lineValue, Context context) throws IOException, InterruptedException {

            log.debug("map task started for job: " + context.getJobName() + " on host: " +  InetAddress.getLocalHost().getHostName());

//            String blastOutputFilePath = context.getConfiguration().get("blastoutputfile");
            Boolean skipExecution = context.getConfiguration().getBoolean("assembly.skipexecution", false);
            
//            context.getCounter(AssemblyCounters.NUMBER_OF_READS).increment(value.size());

            if (skipExecution) {
                /*
                print some diagnostics instead of executing blat
                 */

                System.out.println("Running Assembly task");
                System.out.println("\thost = " + InetAddress.getLocalHost().getHostName() );
                System.out.println("\treadset size = " + lineValue);

            }
            /*
            execute the blast command
             */
            Map<String,String> s = null;
            Map<String,String> map = new HashMap<String,String>();

            String[] a = lineValue.toString().split("\t",2);
            String groupId = a[0];
            for (String read : a[1].split("\t")) {
                String[] b = read.split("&",2);
                map.put(b[0],b[1]);
            }
            try {
                if (!skipExecution)
                    s = assemblerCmd.exec(groupId, map, context);
                else
                    s = new HashMap<String,String>();
            } catch (Exception e) {
                /*
                something bad happened.  update the counter and throw exception
                 */
                log.error(e);
//                context.getCounter(AssemblyCounters.NUMBER_OF_ERROR_BLATCOMMANDS).increment(1);
                throw new IOException(e);
            }

            /*
            blast must have been successful
             */
            //context.getCounter(AssemblyCounters.NUMBER_OF_SUCCESSFUL_BLATCOMMANDS).increment(1);
            //context.getCounter(AssemblyCounters.NUMBER_OF_MATCHED_READS).increment(s.size());

            log.debug("assembler retrieved " + s.size() + " results");

            for (String k : s.keySet()) {

                context.write(new Text(k), new Text(s.get(k)));

            }

            context.setStatus("Completed");
        }
    }

    /**
     * simple reducer that just outputs the matches grouped by gene
     */
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text>
    {
        private IntWritable result = new IntWritable();

        Logger log = Logger.getLogger(IntSumReducer.class);

        /**
         * initialization of mapper retrieves connection parameters from context and opens socket
         * to cassandra data server
         *
         * @param context is the hadoop reducer context
         */
        protected void setup(Reducer.Context context) throws UnknownHostException {

            log.debug("initializing reducer class for job: " + context.getJobName());
            log.debug("\tinitializing reducer on host: " + InetAddress.getLocalHost().getHostName());

        }

        /**
         * free resource after mapper has finished, ie close socket to cassandra server
         *
         * @param context the reducer context
         */
        protected void cleanup(Reducer.Context context) {

            /* void */

        }

        /**
         * main reduce step, simply string concatenates all values of a particular key with tab as seperator
         */
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws InterruptedException, IOException {

//            Text reads = new Text();

//            context.getCounter(AssemblyCounters.NUMBER_OF_GROUPS).increment(1);

            log.debug("running reducer class for job: " + context.getJobName());
            log.debug("\trunning reducer on host: " + InetAddress.getLocalHost().getHostName());

            for (Text t : values){
                context.write(key, t);
            }

        }
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

        conf.setInt("io.file.buffer.size", 1024 * 1024);

        log.info("main() [version " + conf.getStrings("version", "unknown!")[0] + "] starting with following parameters");
        log.info("\tblat results file: " + otherArgs[0]);
        log.info("\tassembly.cleanup : " + conf.getBoolean("assembly.cleanup", true));
        log.info("\tassembly.skipexecution: " + conf.getBoolean("assembly.skipexecution", false));

        /*
        setup blast configuration parameters
         */

        Job job = new Job(conf, "assembler");
        job.setJarByClass(Assembler.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(LineTokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}



