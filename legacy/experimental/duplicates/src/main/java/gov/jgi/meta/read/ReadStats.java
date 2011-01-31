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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
 */
enum ReadCounters {
   MALFORMED,
   READCOUNT
}

/**
 * hadoop application to read sequences from cassandra and generate some simple stats
 */
public class ReadStats {

    /**
     * map task reads fasta sequences from cassandra and
     * generates the kmers and inserts them directly into the cassandra datastore.
     *
     */
    public static class TokenizerMapper extends Mapper<String, SortedMap<byte[], IColumn>, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        String columnFamily = null;
        String readTableName = null;

        Logger log = Logger.getLogger(TokenizerMapper.class);


        /**
         * initialization of mapper retrieves connection parameters from context and opens socket
         * to cassandra data server
         *
         * @param context
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        protected void setup(Mapper.Context context)
                throws IOException, InterruptedException
        {

            log.info("initializing mapper class for job: " + context.getJobName());
            log.info("\tinitializing mapper on host: " + InetAddress.getLocalHost().getHostName());

            readTableName = context.getConfiguration().get("readtablename");
            columnFamily = context.getConfiguration().get("columnfamily");
        }

        /**
         * the map function, processes a single record where record = reads...
         * and generates some simple statistics as output.
         *
         */
        public void map(String key, SortedMap<byte[], IColumn> columns, Context context)
                throws IOException, InterruptedException {

            int count = columns.size();
            log.debug("\tkey/size = " + key + "/" + count);
            log.debug("\thostname = " + InetAddress.getLocalHost().getHostName());

            log.info("inside TokenizerMapper:Map() function: key = " + key + "/" + readTableName);
             IColumn column = columns.get("sequence".getBytes());
             if (column == null)
                 return;

            if (column.getSubColumn("1".getBytes()) == null ||
                    column.getSubColumn("2".getBytes()) == null) {
                log.info("error: sequence not found");
                return;
            }
             String value = new String(column.getSubColumn("1".getBytes()).value()).substring(0,20) +
                 new String(column.getSubColumn("2".getBytes()).value()).substring(0,20);

             word.set(value);
             context.write(word, one);
            context.getCounter(ReadCounters.READCOUNT).increment(1);

        }

        protected void cleanup(Context context) {

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

            log.info("initializing reducer class for job: " + context.getJobName());
            log.info("\tinitializing reducer on host: " + InetAddress.getLocalHost().getHostName());

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
            int sum = 0;

            log.info("reducing key = " + key);

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
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.addResource("readStats-conf.xml");  // set application properties
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Logger log = Logger.getLogger(ReadStats.class);

        /*
        process arguments
         */

        if (otherArgs.length != 1) {
            System.err.println("Usage: readStats <outputdir>");
            System.exit(2);
        }

        conf.set("cassandrahost", conf.getStrings("cassandrahost", "localhost")[0]);
        conf.setInt("cassandraport", Integer.parseInt(conf.getStrings("cassandraport", "9160")[0]));
        conf.set("readtablename",  conf.getStrings("readtablename", "reads")[0]);
        conf.set("columnfamily",  conf.getStrings("columnfamily", "jgi")[0]);

        log.info("main() [version " + conf.getStrings("version", "unknown!")[0] + "] starting with following parameters");
        log.info("\tcassandrahost: " + conf.get("cassandrahost"));
        log.info("\tcassandraport: " + conf.getInt("cassandraport", 9160));
        log.info("\treadtablename: " + conf.get("readtablename"));
        log.info("\tcolumnfamily : " + conf.get("columnfamily"));

        /*
        setup configuration parameters
         */

        Job job = new Job(conf, "readstats");
        job.setJarByClass(ReadStats.class);
        job.setInputFormatClass(ColumnFamilyInputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(10);

        ConfigHelper.setColumnFamily(job.getConfiguration(), conf.get("columnfamily"), conf.get("readtablename"));
        //ConfigHelper.setInputSplitSize(job.getConfiguration(), 100000);
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(new byte[0], new byte[0],false, 2));
        ConfigHelper.setSlicePredicate(job.getConfiguration(), predicate);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}



