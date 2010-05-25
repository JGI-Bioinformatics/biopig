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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import gov.jgi.meta.cassandra.DataStore;
import gov.jgi.meta.exec.BlatCommand;
import gov.jgi.meta.hadoop.input.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.log4j.Logger;


/*
todo: 1. fix the paired/nonpaired problems
todo: 2. make sure the blat counters are read correctly
todo: 3. document code.
todo: 4. document the configuration parameters
 */


/**
 * custom counters
 */
enum BlatCounters {
    /**
     * number of blat commands that return sucessfully
     */
    NUMBER_OF_SUCCESSFUL_BLATCOMMANDS,
    /**
     * number of blat commands that return with error
     */
    NUMBER_OF_ERROR_BLATCOMMANDS,
    /**
     * total number of reads in the database.
     */
    NUMBER_OF_READS,
    /**
     * total number of reads that matched in all groups
     * (duplicates possible, eg, read x matches to multiple
     * groups).
     */
    NUMBER_OF_MATCHED_READS,
    /**
     * total number of gene groups after blat expansion
     * (should be same as number of groups that were read in)
     */
    NUMBER_OF_GROUPS,
    /**
     * total number of map tasks
     */
    NUMBER_OF_MAP_TASKS,
    /**
     * total number of reduce tasks
     */
    NUMBER_OF_REDUCE_TASKS

}


/**
 * hadoop application to read sequence reads from file perform BLAT
 * operations against constant database.
 * <p/>
 * Set the following properties in a file called: blat-conf.xml that should
 * be in the classpath of hadoop.
 * <p/>
 */
public class BlatFilter {

    /**
     * map class that maps blast output, to expanded set of reads using
     * blat executable.
     */
    public static class FastaTokenizerMapper
            extends Mapper<Object, Map<String, String>, Text, Text> {

        Logger log = Logger.getLogger(FastaTokenizerMapper.class);

        /**
         * blast command wrapper
         */
        BlatCommand blatCmd = null;

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

            blatCmd = new BlatCommand(context.getConfiguration());

            context.getCounter(BlatCounters.NUMBER_OF_MAP_TASKS).increment(1);
        }

        /**
         * free resource after mapper has finished, ie close socket to cassandra server
         *
         * @param context is the job context for the map task
         */
        protected void cleanup(Context context) throws IOException {

            log.debug("deleting map task for job: " + context.getJobName() + " on host: " + InetAddress.getLocalHost().getHostName());

        }


        /**
         * the map function processes a block of fasta reads through the blast program
         *
         * @param key     - unused (just junk)
         * @param value   - a map of <readid, sequence>'s
         * @param context - configuration context
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        public void map(Object key, Map<String, String> value, Context context) throws IOException, InterruptedException {

            log.debug("map task started for job: " + context.getJobName() + " on host: " + InetAddress.getLocalHost().getHostName());

            String blastOutputFilePath = context.getConfiguration().get("blastoutputfile");
            Boolean skipExecution = context.getConfiguration().getBoolean("blat.skipexecution", false);

            context.getCounter(BlatCounters.NUMBER_OF_READS).increment(value.size());

            /*
            execute the blast command
             */
            Set<String> s = null;

            try {
                if (!skipExecution)
                    s = blatCmd.exec(value, blastOutputFilePath, context);
                else
                    s = new HashSet<String>();
            } catch (Exception e) {
                /*
                something bad happened.  update the counter and throw exception
                 */
                log.error(e);
                context.getCounter(BlatCounters.NUMBER_OF_ERROR_BLATCOMMANDS).increment(1);
                throw new IOException(e);
            }
            if (s == null) {
                log.info("unable to retrieve results of blat execution");
                context.getCounter(BlatCounters.NUMBER_OF_ERROR_BLATCOMMANDS).increment(1);
            }

            /*
            blat must have been successful
             */
            context.getCounter(BlatCounters.NUMBER_OF_SUCCESSFUL_BLATCOMMANDS).increment(s.size());


            log.debug("blat retrieved " + s.size() + " results");

            for (String k : s) {

                /*
                blat returns the stdout, line by line.  the output is split by tab and
                the first column is the id of the gene, second column is the read id
                 */
                String[] a = k.split(", ", 2);
                Text groupkey = new Text(a[0]);
                String[] sequences = a[1].split(", ");

                context.getCounter(BlatCounters.NUMBER_OF_MATCHED_READS).increment(sequences.length);

                for (String seqid : sequences) {
                    context.write(groupkey, new Text(seqid + "&" + value.get(seqid)));
                }

            }

            context.setStatus("Completed");
        }
    }

    /**
     * simple reducer that just outputs the matches grouped by gene
     */
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        Logger log = Logger.getLogger(IntSumReducer.class);

        /**
         * initialization of mapper retrieves connection parameters from context and opens socket
         * to cassandra data server
         *
         * @param context is the hadoop reducer context
         */
        protected void setup(Reducer.Context context) throws UnknownHostException {

            context.getCounter(BlatCounters.NUMBER_OF_REDUCE_TASKS).increment(1);

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

            Text reads = new Text();

            context.getCounter(BlatCounters.NUMBER_OF_GROUPS).increment(1);

            log.debug("running reducer class for job: " + context.getJobName());
            log.debug("\trunning reducer on host: " + InetAddress.getLocalHost().getHostName());

            int i = 0;
//            context.write(key, new Text(values.toString()));
            for (Text t : values) {
                if (i > 0) {
                    reads.append(("\t" + t).getBytes(), 0, t.getLength() + 1);
                } else {
                    reads.append(t.getBytes(), 0, t.getLength());
                }
                i++;
            }

            context.write(key, reads);

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

        conf.addResource("blat-conf.xml");  // application configuration properties
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Logger log = Logger.getLogger(BlatFilter.class);

        /*
        process arguments
         */

        if (otherArgs.length != 3) {
            System.err.println("Usage: blat <readfile> <blatoutputfile> <outputdir>");
            System.exit(2);
        }

        conf.set("blastoutputfile", otherArgs[1]);
        conf.setInt("io.file.buffer.size", 1024 * 1024);

        log.info("main() [version " + conf.get("version", "unknown!")+ "] starting with following parameters");
        log.info("\tsequence file: " + otherArgs[0]);
        log.info("\tgene db file : " + otherArgs[1]);
        log.info("\tblat.cleanup : " + conf.getBoolean("blat.cleanup", true));
        log.info("\tblat.skipexecution: " + conf.getBoolean("blat.skipexecution", false));
        log.info("\tblat.numreducers: " + conf.getInt("blat.numreducers", 1));
        /*
        setup blast configuration parameters
         */

        Job job = new Job(conf, "loader");
        job.setJarByClass(BlatFilter.class);
        job.setInputFormatClass(FastaBlockInputFormat.class);
        job.setMapperClass(FastaTokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(conf.getInt("blat.numreducers", 1));

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}



