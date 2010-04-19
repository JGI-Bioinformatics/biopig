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

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
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
 * hadoop application to read kmers from cassandra and generate some simple stats
 */
public class KmerStatsFromCassandra {

    /**
     * map task reads fasta sequences from cassandra and
     * generates the kmers and inserts them directly into the cassandra datastore.
     *
     */
    public static class TokenizerMapper extends Mapper<String, SortedMap<byte[], IColumn>, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        Logger log = Logger.getLogger(TokenizerMapper.class);

        /*
        cassandra configuration parameters
         */
        TTransport tr = null;
        TProtocol proto = null;
        Cassandra.Client client = null;
        String kmerTableName = null;
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
                kmerTableName = context.getConfiguration().get("kmertablename");

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
         * the map function, processes a single record where record = <kmerhash, <readid, pos>, <readid, pos> etc>
         * and generates some simple statistics as output.
         *
         */
        public void map(String key, SortedMap<byte[], IColumn> columns, Context context)
                throws IOException, InterruptedException {

            log.debug("\tkey = " + key);
            log.debug("\thostname = " + InetAddress.getLocalHost().getHostName());

            Set s=columns.entrySet();

            Iterator i=s.iterator();
            int count = s.size();

            while(i.hasNext())
            {

                Map.Entry m =(Map.Entry)i.next();
                String readid = new String((byte []) m.getKey());
                IColumn column = (IColumn) m.getValue();
                int position = byteArrayToInt(column.value());

                log.debug(key + "[" + readid + "]" + " = " + position);

                word.set(readid);
                context.write(word, new IntWritable(position));
            }


        }


        /**
         * clear current batched operations.
         *
         */
        private void clear() {
            mutation_map = new HashMap();
        }

        /**
         * stages new data operation for later committing.  inserts key[column] = value into the
         * hash table.
         *
         * @param key
         * @param column
         * @param value
         * @throws IOException
         */
        private void insert(String tablename, String key, String column, int value) throws IOException {

            if (mutation_map == null) {
                clear();
            }

            long timestamp = System.currentTimeMillis();

            if (mutation_map.get(key) == null) {
                mutation_map.put(key, new HashMap());
                ((HashMap) mutation_map.get(key)).put(tablename, new LinkedList());
            }

            Mutation kmerinsert = new Mutation();

            byte[] b = intToByteArray(value);

            ColumnOrSuperColumn c = new ColumnOrSuperColumn();
            c.setColumn(new Column(column.getBytes(), b, timestamp));
            kmerinsert.setColumn_or_supercolumn(c);

            ((List) ((HashMap) mutation_map.get(key)).get(tablename)).add(kmerinsert);

        }

        /**
         * flush data store operations to cassandra server, return (roughly) the number of bytes
         * sent.
         *
         * @return the number of bytes sent (roughly)
         * @throws IOException
         */
        private int commit(String keyspace) throws IOException {

            try {
                client.batch_mutate(keyspace, mutation_map, ConsistencyLevel.ONE);
            } catch (Exception e) {
                throw new IOException(e);
            }
            return mutation_map.toString().length();

        }

        /**
         * convet int to byte array assuming 8 bytes per integer
         *
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

        /*
        * Convert the byte array to an int.
        *
        * @param b The byte array
        * @return The integer
        */

        public static int byteArrayToInt(byte[] b) {
            return byteArrayToInt(b, 0);
        }

        /**
         * Convert the byte array to an int starting from the given offset.
         *
         * @param b      The byte array
         * @param offset The array offset
         * @return The integer
         */
        public static int byteArrayToInt(byte[] b, int offset) {
            int value = 0;
            for (int i = 0; i < 8; i++) {
                int shift = (8 - 1 - i) * 8;
                value += (b[i + offset] & 0x000000FF) << shift;
            }
            return value;
        }

    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int min = 100000000;

            //logger.info("inside IntSumReducer... karan");

            for (IntWritable val : values)
            {
                if (val.get() < min) min = val.get();
            }

            result.set(min);
            context.write(key, result);
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
        
        conf.addResource("kmerStats-conf.xml");  // set kmer application properties
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Logger log = Logger.getLogger(KmerStatsFromCassandra.class);

        /*
        process arguments
         */

        if (otherArgs.length != 2) {
            System.err.println("Usage: kmerstatsfromcassandra <kmerhashtable> <outputdir>");
            System.exit(2);
        }

        conf.set("cassandrahost", conf.getStrings("cassandrahost", "localhost")[0]);
        conf.setInt("cassandraport", Integer.parseInt(conf.getStrings("cassandraport", "9160")[0]));
        conf.set("kmertablename", otherArgs[0]);

        log.info("main() [version " + conf.getStrings("version", "unknown!")[0] + "] starting with following parameters");
        log.info("\tcassandrahost: " + conf.get("cassandrahost"));
        log.info("\tcassandraport: " + conf.getInt("cassandraport", 9160));
        log.info("\tkmer table   : " + otherArgs[0]);
        log.info("\toutput file  : " + otherArgs[1]);

        /*
        setup configuration parameters
         */

        Job job = new Job(conf, "kmerstatsfromcassandra");
        job.setJarByClass(KmerStatsFromCassandra.class);
        job.setInputFormatClass(ColumnFamilyInputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1); 

        ConfigHelper.setColumnFamily(job.getConfiguration(), "jgi", "hash");
        //ConfigHelper.setInputSplitSize(job.getConfiguration(), 10);
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(new byte[0], new byte[0],false,10));
        ConfigHelper.setSlicePredicate(job.getConfiguration(), predicate);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}



