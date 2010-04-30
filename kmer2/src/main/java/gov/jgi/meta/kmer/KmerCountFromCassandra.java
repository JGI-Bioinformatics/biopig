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


import gov.jgi.meta.cassandra.DataStore;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
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
 * hadoop application to read sequence reads from cassandra, generate the unique kmers
 * and insert back into cassandra.  reads from the "read" table, and inserts kmers into
 * the "hash" table.  see discussion of datamodel.
 *
 * Configuration parameters are set in a properties file called "kmer2-conf.xml" and should be
 * accessible from the classpath.
 *
 *    - version
 *      0.1
 *    - cassandrahost
 *      paris
 *    - cassandraport
 *      9160
 *    - mapred.max.split.size
 *      3000000
 *    - keyspace
 *      jgi
 *    - readtablename
 *      reads
 *    - kmertablename
 *      hash
 *    - inputsplitsize
 *      100000
 */
public class KmerCountFromCassandra {

    /**
     * map task reads fasta sequences from cassandra and
     * generates the kmers and inserts them directly into the cassandra datastore.
     *
     */
    public static class TokenizerMapper extends Mapper<String, SortedMap<byte[], IColumn>, Text, IntWritable>
    {
        static int DEFAULTKMERSIZE = 20;

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        Logger log = Logger.getLogger(TokenizerMapper.class);

        DataStore ds = null;
        String readtablename = null;
        String kmertablename = null;

        /*
        kmer options
         */
        int k = DEFAULTKMERSIZE;

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
            log.info("\tkeyspace = " + context.getConfiguration().get("keyspace"));
            log.info("\treadtablename = " + context.getConfiguration().get("readtablename"));
            log.info("\tkmertablename = " + context.getConfiguration().get("kmertablename"));
            log.info("\tusing kmer option k = " + context.getConfiguration().getInt("k", DEFAULTKMERSIZE));
            log.info("\tdatahostmapping = " + context.getConfiguration().get("datahostmapping"));

            ds = new DataStore(context.getConfiguration());

            k = context.getConfiguration().getInt("k", DEFAULTKMERSIZE);
            readtablename = context.getConfiguration().get("readtablename");
            kmertablename = context.getConfiguration().get("kmertablename");
        }

        /**
         * free resource after mapper has finished, ie close socket to cassandra server
         *
         * @param context
         */
        protected void cleanup(Context context) {

            if (ds != null) ds.cleanup();
            
        }


        /**
         * the map function, processes a single record where record = <read id, sequence string>.  mapper generates
         * kmer's of appropriate size and inserts them into the cassandra datastore.
         *
         */
        public void map(String key, SortedMap<byte[], IColumn> columns, Context context)
                throws IOException, InterruptedException {

            LinkedList<String> l = new LinkedList<String>();
            l.add("0"); l.add("1"); l.add("2");

            IColumn column = columns.get("sequence".getBytes());
            log.debug("map function called with sequence = " + new String(column.name()));
            log.debug("\tcontext = " + context.toString());
            log.debug("\tkey = " + key);
            log.debug("\thostname = " + InetAddress.getLocalHost().getHostName());


            for (String segment : l) {
                if (column.getSubColumn(segment.getBytes()) != null) {
                    String sequence = new String(column.getSubColumn(segment.getBytes()).value());

                    if (!sequence.matches("[ATGCN]*")) {
                        log.error("sequence " + key + " is not well formed: " + sequence);
                        context.getCounter(ReadCounters.MALFORMED).increment(1);
                        return;
                    }

                    context.getCounter(ReadCounters.WELLFORMEND).increment(1);

                    int seqsize = sequence.length();
                    int kmersize = k;

                    ds.clear();
                    int i;
                    for (i = 0; i < seqsize - kmersize - 1; i++) {
                        int direction = 1;
                        /*
                        determine both the kmer and its reverse, and use only the lexiographically
                        smaller one as the cannonmical representation of the kmer
                         */
                        String kmer = sequence.substring(i, i + kmersize);
                        String kmerReverse = new StringBuffer(kmer).reverse().toString();
                        if (kmerReverse.compareTo(kmer) < 0) {
                            kmer = kmerReverse;
                            direction = -1;
                        }

                        ds.insert(kmertablename, kmer, key, i * direction);
                    }
                    int numbytessent = ds.commit();

                    context.getCounter(ReadCounters.KMERCOUNT).increment(i);
                    context.getCounter("bandwidth", ds.cassandraHost).increment(numbytessent);
                    //context.getCounter(ReadCounters.BYTESSENTOVERNETWORK).increment(numbytessent);
                }
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
        
        conf.addResource("kmer2-conf.xml");  // set kmer application properties
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Logger log = Logger.getLogger(KmerCountFromCassandra.class);

        /*
        process arguments
         */

        if (otherArgs.length != 2) {
            System.err.println("Usage: kmercountfromcassandra <kmersize> <outputdir>");
            System.exit(2);
        }

        if (conf.get("datahostmapping") != null) conf.set("datahostmapping", conf.get("datahostmapping"));
        conf.set("cassandrahost", conf.getStrings("cassandrahost", "localhost")[0]);
        conf.setInt("cassandraport", Integer.parseInt(conf.getStrings("cassandraport", "9160")[0]));
        conf.set("keyspace", conf.getStrings("keyspace", "jgi")[0]);
        conf.set("readtablename", conf.getStrings("readtablename", "reads")[0]);
        conf.set("kmertablename", conf.getStrings("kmertablename", "hash")[0]);
        conf.setInt("k", Integer.parseInt(otherArgs[0]));

        log.info("main() [version " + conf.getStrings("version", "unknown!")[0] + "] starting with following parameters");
        log.info("\tcassandrahost: " + conf.get("cassandrahost"));
        log.info("\tcassandraport: " + conf.getInt("cassandraport", 9160));
        log.info("\tkeyspace     : " + conf.get("keyspace"));
        log.info("\tread table   : " + conf.get("readtablename"));
        log.info("\tkmer table   : " + conf.get("kmertablename"));
        log.info("\tk            : " + Integer.parseInt(otherArgs[0]));
        log.info("\toutput file  : " + otherArgs[1]);
        log.info("\tinputsplitsize:" + conf.getInt("inputsplitsize", 1000000));
        log.info("\trangebatchsize:" +  conf.getInt("rangebatchsize", 1000000));
        log.info("\tdatahostmapping:" + conf.get("datahostmapping"));

        /*
        setup configuration parameters
         */

        Job job = new Job(conf, "kmerfromcassandra");
        job.setJarByClass(KmerCountFromCassandra.class);
        job.setInputFormatClass(ColumnFamilyInputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(0);  // no reduce tasks needed

        ConfigHelper.setColumnFamily(job.getConfiguration(), conf.get("keyspace"), conf.get("readtablename"));
        ConfigHelper.setInputSplitSize(job.getConfiguration(), conf.getInt("inputsplitsize", 1000000));
        ConfigHelper.setRangeBatchSize(job.getConfiguration(), conf.getInt("rangebatchsize", 100000));
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(new byte[0], new byte[0],false,10));
        ConfigHelper.setSlicePredicate(job.getConfiguration(), predicate);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}



