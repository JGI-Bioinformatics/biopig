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
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import gov.jgi.meta.cassandra.DataStore;
import gov.jgi.meta.exec.BlastCommand;
import gov.jgi.meta.hadoop.input.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.log4j.Logger;

/*
todo: clean up counters
todo: clean up options
todo: add paired/unpaired option
 */

/**
 * custom counters
 */
enum BlastCounters {
    /**
     * the number times the blast commandline is executed successfully
     */
    NUMBER_OF_SUCCESSFUL_BLASTCOMMANDS,

    /**
     * the number of times the blast commandline returns error (non-zero return value)
     */
    NUMBER_OF_ERROR_BLASTCOMMANDS,

    /**
     * the sum of the reads returned by each blast exeuction
     */
    NUMBER_OF_MATCHED_READS,
    NUMBER_OF_MATCHED_READS2,

    /**
     * the total number of gene groups
     */
    NUMBER_OF_GROUPS,

    /**
     * the total number of sequences in the read database
     */
    NUMBER_OF_READS,

    /**
     * total number of map tasks
     */
    NUMBER_OF_MAP_TASKS,

    /**
     * total number of reduce tasks
     */
    NUMBER_OF_REDUCE_TASKS
};


/**
 * hadoop application to read sequence reads from file perform BLAST
 * operations against constant database.
 *
 * see associated application config file for details on configuration
 * parameters
 */
public class ReadBlaster {

    /**
     * map task reads portions of the fasta file provided from the input split and
     * generated the kmers and inserts them directly into the cassandra datastore.
     */
    public static class FastaTokenizerMapper
            extends Mapper<Object, Map<String, String>, Text, Text> {

        Logger log = Logger.getLogger(FastaTokenizerMapper.class);

        /**
         * blast command wrapper
         */
        BlastCommand blastCmd = null;

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

            log.debug("initializing map task for hjob: " + context.getJobName());
            log.debug("initializing maptask on host: " + InetAddress.getLocalHost().getHostName());

            blastCmd = new BlastCommand(context.getConfiguration());

            context.getCounter(BlastCounters.NUMBER_OF_MAP_TASKS).increment(1);
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
            context.getCounter(BlastCounters.NUMBER_OF_READS).increment(value.size());

            String geneDBFilePath = context.getConfiguration().get("genedbfilepath");

            /*
            execute the blast command
             */
            Set<String> s = null;

            try {
                s = blastCmd.exec(value, geneDBFilePath);
            } catch (Exception e) {
                /*
                something bad happened.  update the counter and throw exception
                 */
                log.error(e);
                context.getCounter(BlastCounters.NUMBER_OF_ERROR_BLASTCOMMANDS).increment(1);
                throw new IOException(e);
            }

            /*
            blast executed but did not return sensible values, thow error.
             */
            if (s == null || s.size() <= 1) {
                return;
//                context.getCounter(BlastCounters.NUMBER_OF_ERROR_BLASTCOMMANDS).increment(1);
//                log.error("blast did not execute correctly");
//                throw new IOException("blast did not execute properly");
            }

            /*
            blast must have been successful
             */
            context.getCounter(BlastCounters.NUMBER_OF_SUCCESSFUL_BLASTCOMMANDS).increment(1);
            context.getCounter(BlastCounters.NUMBER_OF_MATCHED_READS).increment(s.size());

            log.debug("blast retrieved " + s.size() + " results");

            for (String k : s) {

                /*
                blast returns the stdout, line by line.  the output is split by tab and
                the first column is the id of the gene, second column is the read id
                 */
                String[] a = k.split("\t");

                /*
                note that we strip out the readid direction.  that is, we don't care if the
                read is a forward read (id/1) or backward (id/2).
                 */
                context.write(new Text(a[0]), new Text(a[1].split("/")[0]));


            }
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

            log.debug("initializing reducer class for job: " + context.getJobName());
            log.debug("\tinitializing reducer on host: " + InetAddress.getLocalHost().getHostName());

            context.getCounter(BlastCounters.NUMBER_OF_REDUCE_TASKS).increment(1);

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

            StringBuilder sb = new StringBuilder();

            context.getCounter(BlastCounters.NUMBER_OF_GROUPS).increment(1);

            log.debug("running reducer class for job: " + context.getJobName());
            log.debug("\trunning reducer on host: " + InetAddress.getLocalHost().getHostName());

            int i = 0;
            for (Text t : values) {
                if (i++ == 0) {
                    sb.append(t.toString());
                } else {
                    sb.append("\t").append(t.toString());
                }
            }
            context.getCounter(BlastCounters.NUMBER_OF_MATCHED_READS2).increment(i);
            context.write(key, new Text(sb.toString()));

        }
    }

    /**
     * starts off the hadoop application
     *
     * @param args specify input file cassandra host and kmer size
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Logger log = Logger.getLogger(ReadBlaster.class);

        /*
        load the application configuration parameters (from deployment directory)
         */
        
        Configuration conf = new Configuration();

        /*
        first load the configuration from the build properties (typically packaged in the jar)
         */
        try {
            Properties buildProperties = new Properties();
            buildProperties.load(ClassLoader.getSystemResource("build.properties").openStream());
            for (Enumeration e = buildProperties.propertyNames(); e.hasMoreElements() ;) {
                String k = (String) e.nextElement();
                System.out.println("setting " + k + " to " + buildProperties.getProperty(k));
                System.setProperty(k, buildProperties.getProperty(k));
                
                if (k.matches("^meta.*")) {
                    System.out.println("overriding property: " + k);
                    conf.set(k, buildProperties.getProperty(k));
                }
            }

        } catch (Exception e) {
            System.out.println("unable to find build.properties: " + e);
        }

        /*
        override properties with the deployment descriptor
         */
        conf.addResource("blast-conf.xml");


        /*
        override properties from user's preferences defined in ~/.meta-prefs
         */


        try {
            java.io.FileInputStream fis = new java.io.FileInputStream(new java.io.File(System.getenv("HOME") + "/.meta-prefs"));
            Properties props = new Properties();
            props.load(fis);
            for (Enumeration e = props.propertyNames(); e.hasMoreElements() ;) {
                String k = (String) e.nextElement();
                if (k.matches("^meta.*")) {
                    System.out.println("overriding property: " + k);
                    conf.set(k, props.getProperty(k));
                }
            }
        } catch (Exception e) {
            System.out.println("unable to find ~/.meta-prefs ... skipping");
        }



        /*
        finally, allow user to override from commandline
         */
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


        /*
        process arguments
         */
        if (otherArgs.length != 3) {
            System.err.println("Usage: blast <readfile> <genefile> <outputdir>");
            System.exit(2);
        }

        long sequenceFileLength = 0;

        try {
            FileInputStream file = new FileInputStream(otherArgs[0]);
            BufferedReader d
                    = new BufferedReader(new InputStreamReader(file));

            String line;
            line = d.readLine();
            while (line != null) {
                if (line.charAt(0) != '>') sequenceFileLength += line.length();
                line = d.readLine();
            }
            file.close();
        } catch (Exception e) {
            System.err.println(e);
            System.exit(2);
        }

        conf.setLong("effectivedatabasesize", sequenceFileLength);
        conf.set("genedbfilepath", otherArgs[1]);

        /*
        seems to help in file i/o performance
         */
        conf.setInt("io.file.buffer.size", 1024 * 1024);

        log.info(System.getenv("application.name") + "[version " + System.getenv("application.version") + "] starting with following parameters");
        log.info("\tsequence file: " + otherArgs[0]);
        log.info("\tgene db file : " + otherArgs[1]);

        String[] optionalProperties = {
                "mapred.min.split.size",
                "mapred.max.split.size",
                "blast.commandline",
                "blast.commandpath",
                "blast.tmpdir",
                "blast.cleanup",
                "blast.numreducers"
        };

        for (String option : optionalProperties) {
            if (conf.get(option) != null) {
                log.info("\toption " + option + ":\t" + conf.get(option));
            }
        }


        /*
        setup blast configuration parameters
         */

        Job job = new Job(conf, "loader");

        job.setJarByClass(ReadBlaster.class);
        job.setInputFormatClass(FastaBlockInputFormat.class);
        job.setMapperClass(FastaTokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(conf.getInt("blast.numreducers", 1));

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}



