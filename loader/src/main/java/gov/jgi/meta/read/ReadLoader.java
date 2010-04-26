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

package gov.jgi.meta.read;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import gov.jgi.meta.cassandra.DataStore;
import gov.jgi.meta.hadoop.input.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.apache.log4j.Logger;


/**
 * custom counters
 *
 * MALFORMED is the number of ignored reads due to some simple syntax checking
 * WELLFORMED is the number of reads that were read
 * KMERCOUNT is the number of kmers that were generated
 * BYTESSENTOVERNETWORK is the data that is sent to the cassandra data store
 *
 */
enum ReadCounters {
   MALFORMED,
   WELLFORMEND,
   BYTESSENTOVERNETWORK
}

/**
 * hadoop application to read sequence reads from file, generate the unique kmers
 * and insert into cassandra.
 */
public class ReadLoader {

    /**
     * map task reads portions of the fasta file provided from the input split and
     * generated the kmers and inserts them directly into the cassandra datastore.
     *
     */
    public static class FastaTokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private int batchSize = 100;
        private int currentSize = 0;

        Logger log = Logger.getLogger(FastaTokenizerMapper.class);

        DataStore ds = null;

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

            log.info("\tconnecting to cassandra host: " + context.getConfiguration().get("cassandrahost"));

            ds.initialize(context.getConfiguration());
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

                context.getCounter(ReadCounters.MALFORMED).increment(1);
                return;
            }

            context.getCounter(ReadCounters.WELLFORMEND).increment(1);

            int seqsize = sequence.length();

            String key_id = null;
            String segment = null;

            if (key.toString().contains("/")) {
                String[] l = key.toString().split("/");
                key_id = l[0];
                segment = l[1];
            } else {
                key_id = key.toString();
                segment = "0";
            }
            /*
            insert data into cassandra
            */

            ds.insert(key_id, "sequence", segment, sequence);

            if (currentSize > batchSize) {

                int numbytessent = ds.commit();
                context.getCounter(ReadCounters.BYTESSENTOVERNETWORK).increment(numbytessent);
                ds.clear();

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

        conf.addResource("loader-conf.xml");  // set kmer application properties
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Logger log = Logger.getLogger(ReadLoader.class);

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
        job.setJarByClass(ReadLoader.class);
        job.setInputFormatClass(FastaInputFormat.class);
        job.setMapperClass(FastaTokenizerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(0);  // no reduce tasks needed

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}



