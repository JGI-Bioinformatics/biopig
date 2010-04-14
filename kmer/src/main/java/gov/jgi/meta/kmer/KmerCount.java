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

package gov.jgi.meta.kmer;


import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import gov.jgi.meta.hadoop.input.FastaInputFormat;
import org.apache.cassandra.thrift.*;
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
   KMERCOUNT,
   BYTESSENTOVERNETWORK
}

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
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        Logger log = Logger.getLogger(FastaTokenizerMapper.class);

        /*
        cassandra configuration parameters
         */
        TTransport tr = null;
        TProtocol proto = null;
        Cassandra.Client client = null;
        String cassandraTable = null;
        String cassandraHost = null;
        int cassandraPort = 0;

        /*
        batched operations
         */
        Map mutation_map = null;


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

            if (tr == null) {
                cassandraHost = context.getConfiguration().get("cassandrahost");
                cassandraPort = context.getConfiguration().getInt("cassandraport", 9160);
                cassandraTable = context.getConfiguration().get("cassandratable");

                try {
                    tr = new TSocket(cassandraHost, cassandraPort);
                    proto = new TBinaryProtocol(tr);
                    client = new Cassandra.Client(proto);
                    tr.open();
                } catch (Exception e) {
                    log.fatal("ERROR: " + e);
                    throw new IOException("unable to connect to cassandrahost at " + cassandraHost + "/" + cassandraPort);
                }
            }
        }

        /**
         * free resource after mapper has finished, ie close socket to cassandra server
         *
         * @param context
         */
        protected void cleanup(Context context) {
            if (tr != null) {
                tr.close();
            }
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
            int kmersize = context.getConfiguration().getInt("kmersize", 20);

            clear();
            int i;
            for (i = 0; i < seqsize - kmersize - 1; i++) {
                String kmer = sequence.substring(i, i + kmersize);

                insert(kmer, key.toString(), i);

            }
            int numbytessent = commit();

            context.getCounter(ReadCounters.KMERCOUNT).increment(i);
            context.getCounter(ReadCounters.BYTESSENTOVERNETWORK).increment(numbytessent);

        }

        /**
         * clear current batched operations.
         *
         */
        private void clear() {
            mutation_map = new HashMap();
        }

        /**
         * insert new data operation.  inserts key[column] = value
         *
         * @param key
         * @param column
         * @param value
         * @throws IOException
         */
        private void insert(String key, String column, int value) throws IOException {

            if (mutation_map == null) {
                clear();
            }

            long timestamp = System.currentTimeMillis();

            if (mutation_map.get(key) == null) {
                mutation_map.put(key, new HashMap());
                ((HashMap) mutation_map.get(key)).put(cassandraTable, new LinkedList());
            }

            Mutation kmerinsert = new Mutation();

            byte[] b = intToByteArray(value);

            ColumnOrSuperColumn c = new ColumnOrSuperColumn();
            c.setColumn(new Column(column.getBytes(), b, timestamp));
            kmerinsert.setColumn_or_supercolumn(c);

            ((List) ((HashMap) mutation_map.get(key)).get(cassandraTable)).add(kmerinsert);

        }

        /**
         * flush data store operations to cassandra server, return (roughly) the number of bytes
         * sent.
         *
         * @return the number of bytes sent (roughly)
         * @throws IOException
         */
        private int commit() throws IOException {

            try {
                client.batch_mutate("jgi", mutation_map, ConsistencyLevel.ONE);
            } catch (Exception e) {
                throw new IOException(e);
            }
            return mutation_map.toString().length();

        }

        /**
         * convet int to byte array assuming 8 bytes per integer
         * @param value to convert
         * @return a fresh byte array
         */
        public static byte[] intToByteArray(int value) {
                byte[] b = new byte[8];
                for (int i = 0; i < 8; i++) {
                    int offset = (b.length - 1 - i) * 8;
                    b[i] = (byte) ((value >>> offset) & 0xFF);
                }
                return b;
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
        
        conf.addResource("kmer-conf.xml");  // set kmer application properties
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Logger log = Logger.getLogger(KmerCount.class);

        /*
        process arguments
         */

        if (otherArgs.length != 4) {
            System.err.println("Usage: kmercount <in> <kmer size> <cassandra_table> <outputdir>");
            System.exit(2);
        }

        conf.set("cassandrahost", conf.getStrings("cassandrahost", "localhost")[0]);
        conf.setInt("cassandraport", Integer.parseInt(conf.getStrings("cassandraport", "9160")[0]));
        conf.set("cassandratable", otherArgs[2]);
        conf.setInt("kmersize", Integer.parseInt(otherArgs[1]));

        log.info("main() [version " + conf.getStrings("version", "unknown!")[0] + "] starting with following parameters");
        log.info("\tcassandrahost: " + conf.get("cassandrahost"));
        log.info("\tcassandraport: " + conf.getInt("cassandraport", 9160));
        log.info("\tsequence file: " + otherArgs[0]);
        log.info("\tkmersize     : " + Integer.parseInt(otherArgs[1]));
        log.info("\ttable name   : " + otherArgs[2]);

        /*
        setup configuration parameters
         */
        Job job = new Job(conf, "kmer");
        job.setJarByClass(KmerCount.class);
        job.setInputFormatClass(FastaInputFormat.class);
        job.setMapperClass(FastaTokenizerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(0);  // no reduce tasks needed

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}



