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
import java.util.*;

import gov.jgi.meta.hadoop.input.FastaInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


/**
 * hadoop application to read sequence reads from file, generate the unique kmers
 * and insert into cassandra.
 */
public class KmerCount {

    /**
     * map task reads portions of the fasta file provided from the input split and
     * generated the kmers and inserts them directly into the cassandra datastore.
     *
     */
    public static class FastaTokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        Logger log = Logger.getLogger(FastaTokenizerMapper.class);

        /**
         * initialization of mapper retrieves connection parameters from context and opens socket
         * to cassandra data server
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void setup(Context context)
                throws IOException, InterruptedException
        {

            log.info("initializing mapper class for job: " + context.getJobName());
            log.info("\tcontext = " + context.toString());
            log.info("\tinitializing mapper on host: " + InetAddress.getLocalHost().getHostName());
        }

        /**
         * free resource after mapper has finished, ie close socket to cassandra server
         *
         * @param context
         */
        protected void cleanup(Context context) {

        }


        /**
         * the map function, processes a single record where record = <read id, sequence string>.  mapper generates
         * kmer's of appropriate size and inserts them into the cassandra datastore.
         *
         * @param key - read id as defined in the fasta file
         * @param value - sequence string (AATTGGCC...)
         * @param context - configuration context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            log.debug("map function called with value = " + value.toString());
            log.debug("\tcontext = " + context.toString());
            log.debug("\tkey = " + key.toString());
            log.debug("\thostname = " + InetAddress.getLocalHost().getHostName());

            String sequence = value.toString();
            if (!sequence.matches("[ATGCN]*")) {
                log.error("sequence " + key + " is not well formed: " + value);
                
                context.getCounter(KmerCounters.MALFORMED).increment(1);
                return;
            }

            context.getCounter(KmerCounters.WELLFORMEND).increment(1);

            int seqsize = sequence.length();
            int kmersize = context.getConfiguration().getInt("kmersize", 20);

            int i;
            for (i = 0; i < seqsize - kmersize; i++) {
                String kmer = sequence.substring(i, i + kmersize);

                context.write(new Text(kmer), new Text(key.toString()));

            }
            context.getCounter(KmerCounters.KMERCOUNT).increment(i);

        }
    }

       /**
     * simple reducer that just outputs the matches grouped by gene
     */
    public static class IntSumReducer extends Reducer<Text, Text, Text, IntWritable> {

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

            context.getCounter(KmerCounters.NUMBER_OF_REDUCE_TASKS).increment(1);

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

            log.debug("running reducer class for job: " + context.getJobName());
            log.debug("\trunning reducer on host: " + InetAddress.getLocalHost().getHostName());

            int i = 0;
            for (Text t : values) {
                if (i++ > 0) sb.append("\t");
                sb.append(t.toString());
            }
            context.getCounter(KmerCounters.KMERCOUNT2).increment(i);

            //context.write(key, new Text(sb.toString()));
              context.write(key, new IntWritable(i));
        }
    }

    /**
     * starts off the hadoop application
     *
     * @param args specify input file cassandra host and kmer size
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

         Logger log = Logger.getLogger(KmerCount.class);

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

        }

        /*
        override properties with the deployment descriptor
         */
        conf.addResource("kmer-conf.xml");

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
            log.error("unable to find ~/.meta-prefs ... skipping");
        }


        /*
        finally, allow user to override from commandline
         */
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        /*
        process arguments
         */
        if (otherArgs.length != 2) {
            System.err.println("Usage: kmer <readfile> <outputdir>");
            System.exit(2);
        }

        /*
        seems to help in file i/o performance
         */
        conf.setInt("io.file.buffer.size", 1024 * 1024);

        log.info(System.getenv("application.name") + "[version " + System.getenv("application.version") + "] starting with following parameters");
        log.info("\tsequence file: " + otherArgs[0]);

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

        Job job = new Job(conf, "kmer");

        job.setJarByClass(KmerCount.class);
        job.setInputFormatClass(FastaInputFormat.class);
        job.setMapperClass(FastaTokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(conf.getInt("kmer.numreducers", 1));

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



