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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.apache.log4j.Logger;



enum ReadCounters {
   MALFORMED,
   WELLFORMEND
}

/**
 * hadoop application to read sequence reads from file, generate the unique kmers
 * and insert into cassandra.
 */
public class KmerCount {

    /**
     * map task reads portions of the fasta file provided from the input split and
     * generated the kmers and inserts them directly into the cassandra datastore
     */
    public static class FastaTokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        static Logger log = Logger.getLogger(FastaTokenizerMapper.class);
        static TTransport tr = null;
        static TProtocol proto;
        static Cassandra.Client client = null;

        protected void setup(Context context)
                throws IOException, InterruptedException
        {
            log.info("initializing mapper class for job: " + context.getJobName());
            log.info("\tcontext = " + context.toString());
            log.info("\tinitializing mapper on host: " + InetAddress.getLocalHost().getHostName());

            log.info("\tconnecting to cassandra host: " + context.getConfiguration().get("cassandrahost"));

        }


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            log.info("map function called with value = " + value.toString());
            log.info("\tcontext = " + context.toString());
            log.info("\tkey = " + key.toString());
            log.info("\thostname = " + InetAddress.getLocalHost().getHostName());


            context.getCounter(ReadCounters.WELLFORMEND).increment(1);

            /*
            * should get the sequence item, not the line tokenizer... use custom splitter
            */
            //    StringTokenizer itr = new StringTokenizer(value.toString());


/*
            String sequence = value.toString();
            if (!sequence.matches("[ATGCN]*")) return;

            int seqsize = sequence.length();
            int kmersize = 20;


            for (int i = 0; i < seqsize - kmersize - 1; i++) {
                String kmer = sequence.substring(i, i + kmersize);

                word.set(kmer);
                context.write(word, one);
            }
*/
        }
    }


    public static byte[] intToByteArray(long value) {
        byte[] b = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = (b.length - 1 - i) * 8;
            b[i] = (byte) ((value >>> offset) & 0xFF);
        }
        return b;
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        int numRuns = 0;
        TTransport tr;
        TProtocol proto;
        Cassandra.Client client = null;

        protected void setup(Reducer.Context context) {

            Random rand = new Random();
            int min = 0, max = 1;
            String[] hostarray = new String[4];
            hostarray[0] = "paris";
            hostarray[1] = "rome";
            hostarray[2] = "ren";
            hostarray[3] = "stimpy";

            String cassandrahost;

            try {
                InetAddress addr = InetAddress.getLocalHost();

                // Get IP Address
                byte[] ipAddr = addr.getAddress();

                // Get hostname
                String myhostname = addr.getHostName();

                int randomNum = rand.nextInt(max - min + 1) + min;

                if (myhostname.equals("ren")) {
                    cassandrahost = new String("paris");
                } else if (myhostname.equals("stimpy")) {
                    cassandrahost = new String("rome");
                } else {
                    cassandrahost = new String("localhost");
                }
                System.out.println("Reduce setup: running on host" + myhostname + "  using cassandra " + cassandrahost);

                tr = new TSocket(cassandrahost, 9160);
                proto = new TBinaryProtocol(tr);
                client = new Cassandra.Client(proto);

                tr.open();
            } catch (Exception e) {
                System.out.println("ERROR: " + e);
            }

        }

        protected void cleanup(Reducer.Context context)
                throws IOException,
                InterruptedException {


            tr.close();

        }


        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            numRuns += 1;
            int sum = 0;
            String kkey = key.toString();

            Map mutation_map = new HashMap();
            long timestamp = System.currentTimeMillis();
            mutation_map.put(kkey, new HashMap());
            ((HashMap) mutation_map.get(kkey)).put("hash", new LinkedList());

            for (IntWritable val : values) {
                Mutation kmerinsert = new Mutation();
                /*
                 insert data into cassandra
                 */
                byte[] b = intToByteArray(val.get());

                ColumnOrSuperColumn c = new ColumnOrSuperColumn();
                c.setColumn(new Column("count".getBytes(), b, timestamp));
                kmerinsert.setColumn_or_supercolumn(c);

                ((List) ((HashMap) mutation_map.get(kkey)).get("hash")).add(kmerinsert);


//                client.insert(keyspace,
//                        kmer,
//                        new ColumnPath(table).setColumn(key_user_id.getBytes()),
//                        b,
//                        timestamp,
//                        ConsistencyLevel.ONE);


            }

            //println("mutation_map = " + mutation_map.toString());
            try {
                //System.out.println("mutation_map =  " + mutation_map.toString());
                client.batch_mutate("jgi", mutation_map, ConsistencyLevel.ONE);
            } catch (Exception e) {
                System.out.println("Error: " + e);
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
        
        conf.addResource("kmer-conf.xml");  // set kmer application properties
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Logger log = Logger.getLogger(KmerCount.class);

        /*
        process arguments
         */

        if (otherArgs.length != 2) {
            System.err.println("Usage: kmercount <in> <cassandra_table>");
            System.exit(2);
        }

        conf.set("cassandrahost", conf.getStrings("cassandrahost", "localhost")[0]);
        conf.set("cassandraport", conf.getStrings("cassandraport", "9160")[0]);
        conf.set("tablename", otherArgs[1]);

        log.info("main() [version " + conf.getStrings("version", "unknown!")[0] + "] starting with following parameters");
        log.info("\tcassandrahost: " + conf.get("cassandrahost"));
        log.info("\tcassandraport: " + conf.get("cassandraport"));
        log.info("\tsequence file: " + otherArgs[0]);
        log.info("\ttable name   : " + otherArgs[1]);

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
        FileOutputFormat.setOutputPath(job, new Path("/tmp/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

/*
    public int run2(String[] args) throws Exception
    {
        Configuration conf = getConf();

        logger.info("creating " + 1 + "jobs - karan");

        for (int i = 0; i < 1; i++)
        {
            logger.info("Job " + i);

            String columnName = "sequence";

            logger.info("looking at column: " + columnName);

            conf.set(CONF_COLUMN_NAME, columnName);
            Job job = new Job(conf, "wordcount");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setInputFormatClass(ColumnFamilyInputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX + i));

            ConfigHelper.setColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
//            ConfigHelper.setInputSplitSize(job.getConfiguration(), 10);
            SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(columnName.getBytes()));
            ConfigHelper.setSlicePredicate(job.getConfiguration(), predicate);

            job.waitForCompletion(true);
        }
        return 0;
    }
*/

}



