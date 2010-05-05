package gov.jgi.meta;/*
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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import gov.jgi.meta.cassandra.DataStore;
import gov.jgi.meta.exec.BlastCommand;
import gov.jgi.meta.hadoop.input.*;
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

import org.apache.log4j.Logger;

/**
 * hadoop application to read sequence reads from file and inserts into cassandra.
 */
public class ReadBlaster {

    /**
     * map task reads portions of the fasta file provided from the input split and
     * generated the kmers and inserts them directly into the cassandra datastore.
     *
     */
    public static class FastaTokenizerMapper
            extends Mapper<Object, Map<String, String>, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private int batchSize = 100;
        private int currentSize = 0;

        Logger log = Logger.getLogger(FastaTokenizerMapper.class);

        /**
         * abstracts the details of connections to the cassandra servers
         */
        DataStore ds = null;

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
                throws IOException, InterruptedException
        {

            log.info("initializing mapper class for job: " + context.getJobName());
            log.info("\tcontext = " + context.toString());
            log.info("\tinitializing mapper on host: " + InetAddress.getLocalHost().getHostName());

            log.info("\tconnecting to cassandra host: " + context.getConfiguration().get("cassandrahost"));

            ds = new DataStore(context.getConfiguration());

            blastCmd = new BlastCommand(context.getConfiguration());

        }

        /**
         * free resource after mapper has finished, ie close socket to cassandra server
         *
         * @param context
         */
        protected void cleanup(Context context) throws IOException {

            if (ds != null) ds.cleanup();

        }


        /**
         * the map function, processes a single record where record = <read id, sequence string>.  mapper generates
         * kmer's of appropriate size and inserts them into the cassandra datastore.
         *
         * @param key - read id as defined in the fasta file
         * @param value - sequence string (AATTGGCC...)
         * @param context - configuration context
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        public void map(Object key, Map<String, String> value, Context context) throws IOException, InterruptedException {


            // formatdb -i est.fa -o T -p F
            // "blastall -m 8 -p tblastn -b 1000000 -a 10 -o $workdir/cazy.blastout -d $blast_db -i $cazy
            
            log.debug("map function called with value = " + value.toString());
            log.debug("\tcontext = " + context.toString());
            log.debug("\tkey = " + key.toString());
            log.debug("\thostname = " + InetAddress.getLocalHost().getHostName());

            // need to write set of sequences to disk then execute blast

            Set<String> s = blastCmd.exec(value, "/scratch/karan/EC3.2.1.4.faa");

            for (String k : s) {
                context.write(new Text(k), one);
            }
        }
    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        Logger log = Logger.getLogger(IntSumReducer.class);

        /**
         * initialization of mapper retrieves connection parameters from context and opens socket
         * to cassandra data server
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void setup(Reducer.Context context)
                throws IOException, InterruptedException
        {

            log.info("initializing mapper class for job: " + context.getJobName());
            log.info("\tcontext = " + context.toString());
            log.info("\tinitializing mapper on host: " + InetAddress.getLocalHost().getHostName());
            log.info("\tconnecting to cassandra host: " + context.getConfiguration().get("cassandrahost"));

        }

        /**
         * free resource after mapper has finished, ie close socket to cassandra server
         *
         * @param context
         */
        protected void cleanup(Reducer.Context context) {

        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            //int min = 100000000;
            int sum = 0;

            log.debug("inside IntSumReducer... karan");
            log.debug("\tkey = " + key);

            for (IntWritable val : values)
            {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
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

        conf.addResource("loader-conf.xml");  // set kmer application properties
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Logger log = Logger.getLogger(ReadBlaster.class);

        /*
        process arguments
         */

        if (otherArgs.length != 3) {
            System.err.println("Usage: loader <in> <cassandra_table> <outputdir>");
            System.exit(2);
        }

        conf.set("cassandrahost", conf.getStrings("cassandrahost", "localhost")[0]);
        conf.setInt("cassandraport", Integer.parseInt(conf.getStrings("cassandraport", "9160")[0]));
        conf.set("cassandratable", otherArgs[1]);

        log.info("main() [version " + conf.getStrings("version", "unknown!")[0] + "] starting with following parameters");
        log.info("\tcassandrahost: " + conf.get("cassandrahost"));
        log.info("\tcassandraport: " + conf.getInt("cassandraport", 9160));
        log.info("\tsequence file: " + otherArgs[0]);
        log.info("\ttable name   : " + otherArgs[1]);

        /*
        setup configuration parameters
         */
        Job job = new Job(conf, "loader");
        job.setJarByClass(ReadBlaster.class);
        job.setInputFormatClass(FastaBlockInputFormat.class);
        job.setMapperClass(FastaTokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);  // no reduce tasks needed

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}



