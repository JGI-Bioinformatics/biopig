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
import gov.jgi.meta.hadoop.io.ReadNode;
import gov.jgi.meta.hadoop.io.ReadNodeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.biojava.bio.seq.Sequence;


public class Dereplicate {

    public static class PairMapper
            extends Mapper<Text, Sequence, Text, Text> {

        Logger log = Logger.getLogger(AggregateReducer.class);

        public void map(Text seqid, Sequence s, Context context) throws IOException, InterruptedException {
            String sequence = s.seqString();
            if (!sequence.matches("[atgcn]*")) {
                log.error("sequence " + seqid + " is not well formed: " + sequence);
                return;
            }
            String[] seqNameArray = seqid.toString().split("/");
            context.write(new Text(seqNameArray[0]), new Text(sequence));
        }
    }



    public static class PairReducer extends Reducer<Text, Text, Text, Text> {

        Logger log = Logger.getLogger(AggregateReducer.class);

        public Text pairJoin(Iterable<Text> l) {
                   StringBuilder sb = new StringBuilder();
                   sb.append("\n");

                   Iterator<Text> i = l.iterator();
                   if (!i.hasNext()) return new Text(sb.toString());
                   else {
                       sb = sb.append(i.next().toString());
                       if (!i.hasNext()) {
                           log.error("missing pair!");
                           return new Text("");
                       }
                       sb = sb.append(StringUtils.reverse(i.next().toString()));
                   }
                   return new Text(sb.toString());
           }


        public void reduce(Text key, Iterable<Text> values, Context context)
                throws InterruptedException, IOException {

            context.write(new Text(">" + key.toString()), pairJoin(values));

       }
    }

    public static class GraphEdgeMapper
            extends Mapper<Text, Sequence, Text, ReadNode> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        Logger log = Logger.getLogger(GraphEdgeMapper.class);

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



        public void map(Text key, Sequence value, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString().trim();
            log.debug("map function called with value = " + value.toString());
            log.debug("\tcontext = " + context.toString());
            log.debug("\tkey = " + keyStr);
            log.debug("\thostname = " + InetAddress.getLocalHost().getHostName());

            String sequence = value.seqString();
            if (!sequence.matches("[atgcn]*")) {
                log.error("sequence " + keyStr + " is not well formed: " + value);
                return;
            }

            int windowSize = context.getConfiguration().getInt("dereplicate.windowsize", 16);
            int editDistance = context.getConfiguration().getInt("dereplicate.editdistance", 1);


            try {
                String sequenceHashValue = sequence.substring(0, windowSize) + StringUtils.reverse(sequence).substring(0,windowSize);

                context.write(new Text(sequenceHashValue), new ReadNode(keyStr, sequenceHashValue, sequence));
                for (String neighbor : generateAllNeighbors(sequenceHashValue, editDistance)) {
                context.write(new Text(neighbor), new ReadNode(keyStr, sequenceHashValue, sequence));
                }
            }catch (Exception e) {
                log.error("error sequence not correct: id = " + keyStr + " sequence = " + sequence + " \n original message: " + e);
            }


        }

        private Set<String> generateAllNeighbors(String start, int distance) {

            String[] bases = {"a", "t", "g", "c"};
            Set<String> s = new HashSet<String>();

            if (distance == 0) {
                s.add(start);
                return s;
            }

            for (int i = 0; i < start.length(); i++) {

                for (String basePair : bases) {
                    if (basePair.equals(start.substring(i, i))) continue;
                    for (String neighbor : generateAllNeighbors(stringReplaceIth(start, i, basePair), distance-1)) {
                        s.add(neighbor);
                    }

                }

            }

            return s;
        }

        private String stringReplaceIth(String s, int i, String c) {
            
            return s.substring(0,i) + c + s.substring(i+1);

        }
    }




       /**
     * simple reducer that just outputs the matches grouped by gene
     */
    public static class GraphEdgeReducer extends Reducer<Text, ReadNode, Text, ReadNodeSet> {

        Logger log = Logger.getLogger(GraphEdgeReducer.class);

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

        private Text textJoin(Iterable<ReadNode> l, String s) {
            StringBuilder sb = new StringBuilder();

            Iterator<ReadNode> i = l.iterator();
            if (!i.hasNext()) return new Text(sb.toString());
            else sb = sb.append(i.next().toString());

            while ( i.hasNext() ){
                sb = sb.append(s).append(i.next().toString());
            }
            return new Text(sb.toString());
        }

        public void reduce(Text key, Iterable<ReadNode> values, Context context)
                throws InterruptedException, IOException {

            HashSet<ReadNode> hs = new HashSet<ReadNode>();

            for (ReadNode r : values) {
                hs.add(new ReadNode(r));
            }

            String keyStr = key.toString();

            boolean found = false;

            for (ReadNode r : hs) {

                if (keyStr.equals(r.hash)) {
                    found = true;
                }

            }

            if (found) {

                context.write(key, new ReadNodeSet(hs));

            }
        }


       }


public static class AggregateMapper
            extends Mapper<LongWritable, Text, ReadNode, Text> {

        Logger log = Logger.getLogger(AggregateMapper.class);

        public void map(LongWritable count, Text line, Context context) throws IOException, InterruptedException {

            String[] lineArray = line.toString().split("\t");
            String hash = lineArray[0];
            ReadNodeSet rs = new ReadNodeSet(lineArray[1]);

            for (ReadNode r : rs.s) {
                context.write(r, new Text(rs.canonicalName()));
            }
        }
}


    public static class AggregateReducer extends Reducer<ReadNode, Text, ReadNode, Text> {

        Logger log = Logger.getLogger(AggregateReducer.class);

        private Text textJoin(Iterable<Text> l, String s) {
            StringBuilder sb = new StringBuilder();

            Iterator<Text> i = l.iterator();
            if (!i.hasNext()) return new Text(sb.toString());
            else sb = sb.append(i.next().toString());

            while ( i.hasNext() ){
                sb = sb.append(s).append(i.next().toString());
            }
            return new Text(sb.toString());
        }
        public void reduce(ReadNode key, Iterable<Text> values, Context context)
                throws InterruptedException, IOException {

            int max = 0;
            Text rsMax = null;
            for (Text rs : values) {
                String[] rsComponents = rs.toString().split(",");
                if (rsComponents.length > max) {
                    max = rsComponents.length;
                    rsMax = rs;
                }
            }

            context.write(key, rsMax);

       }
    }


public static class ChooseMapper
            extends Mapper<LongWritable, Text, Text, ReadNode> {

        Logger log = Logger.getLogger(AggregateMapper.class);

        public void map(LongWritable count, Text line, Context context) throws IOException, InterruptedException {

            String[] lineArray = line.toString().split("\t");
            ReadNode r = new ReadNode(lineArray[0]);
            Text rsName = new Text(lineArray[1]);

            context.write(rsName, r);

        }
}


    public static class ChooseReducer extends Reducer<Text, ReadNode, Text, Text> {

        Logger log = Logger.getLogger(AggregateReducer.class);

        private Text textJoin(Iterable<Text> l, String s) {
            StringBuilder sb = new StringBuilder();

            Iterator<Text> i = l.iterator();
            if (!i.hasNext()) return new Text(sb.toString());
            else sb = sb.append(i.next().toString());

            while ( i.hasNext() ){
                sb = sb.append(s).append(i.next().toString());
            }
            return new Text(sb.toString());
        }

        public void reduce(Text key, Iterable<ReadNode> values, Context context)
                throws InterruptedException, IOException {

            // determine consensus sequence and output fasta formatted file
            // key is the group canonical name
            // values are the set of read nodes

            ReadNodeSet rs = new ReadNodeSet(values);

            context.write(new Text(rs.fastaHeader()), new Text(rs.fastaConsensusSequence()));

       }
    }


    /**
     * starts off the hadoop application
     *
     * @param args specify input file cassandra host and kmer size
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

         Logger log = Logger.getLogger(Dereplicate.class);

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
        conf.addResource("dereplicate-conf.xml");

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
            System.err.println("Usage: dereplicate <readfile> <outputdir>");
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
                "dereplicate.numreducers",
                "dereplicate.editdistance",
                "dereplicate.windowsize"
        };

        for (String option : optionalProperties) {
            if (conf.get(option) != null) {
                log.info("\toption " + option + ":\t" + conf.get(option));
            }
        }


        Job job0 = new Job(conf, "dereplicate-step0");

        job0.setJarByClass(Dereplicate.class);
        job0.setInputFormatClass(FastaInputFormat.class);
        job0.setMapperClass(PairMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job0.setReducerClass(PairReducer.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(Text.class);
        job0.setNumReduceTasks(conf.getInt("dereplicate.numreducers", 1));

        FileInputFormat.addInputPath(job0, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job0, new Path(otherArgs[1]+"/step0"));

        job0.waitForCompletion(true);

        /*
        setup first job configuration parameters
         */

        Job job = new Job(conf, "dereplicate-step1");

        job.setJarByClass(Dereplicate.class);
        job.setInputFormatClass(FastaInputFormat.class);
        job.setMapperClass(GraphEdgeMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(GraphEdgeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ReadNode.class);
        job.setNumReduceTasks(conf.getInt("dereplicate.numreducers", 1));

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]+"/step0"));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"/step1"));

        job.waitForCompletion(true);

        /*
        now setup groups
         */
        Job job2 = new Job(conf, "dereplicate-step2");

        job2.setJarByClass(Dereplicate.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapperClass(AggregateMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(AggregateReducer.class);
        job2.setOutputKeyClass(ReadNode.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(conf.getInt("dereplicate.numreducers", 1));

        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"/step1"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/step2"));


        job2.waitForCompletion(true);

        /*
        now setup groups
         */
        Job job3 = new Job(conf, "dereplicate-step2");

        job3.setJarByClass(Dereplicate.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setMapperClass(ChooseMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job3.setReducerClass(ChooseReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(ReadNode.class);
        job3.setNumReduceTasks(conf.getInt("dereplicate.numreducers", 1));

        FileInputFormat.addInputPath(job3, new Path(otherArgs[1]+"/step2"));
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]+"/step3"));

        job3.waitForCompletion(true);

    }
}



