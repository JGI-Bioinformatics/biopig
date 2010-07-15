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


import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import gov.jgi.meta.hadoop.input.FastaInputFormat;
import gov.jgi.meta.hadoop.io.ReadNode;
import gov.jgi.meta.hadoop.io.ReadNodeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

        Logger log = Logger.getLogger(this.getClass());

        int mapcount = 0;

        public void map(Text seqid, Sequence s, Context context) throws IOException, InterruptedException {
            String sequence = s.seqString();
            mapcount++;

            String[] seqNameArray = seqid.toString().split("/");

            context.write(new Text(seqNameArray[0]), new Text(sequence+"/"+seqNameArray[1]));
        }
    }



    public static class PairReducer extends Reducer<Text, Text, Text, Text> {

        Logger log = Logger.getLogger(AggregateReducer.class);

        public Text pairJoin(Iterable<Text> l) {
                   StringBuilder sb = new StringBuilder();
                   sb.append("\n");
                   HashSet<Text> s = new HashSet<Text>();
                   Iterator<Text> i = l.iterator();
                   int count = 0;
                   String front = "";
                   String back = "";

                   while (i.hasNext()) {
                       count++;
                       String[] x = i.next().toString().split("/");
                       if (x.length != 2) {
                           log.error("key malformed: " + x);
                           return new Text("");
                       }
                       if ("1".equals(x[1])) {
                           front = x[0];
                       } else if ("2".equals(x[1])) {
                           back = x[0];
                       } else {
                           log.error("key malformed: " + x);
                           return new Text("");
                       }
                   }

                   if (count != 2) {
                       log.error("bad key/pair... size = " + count);
                       log.error("front = " + front);
                       log.error("back = " + back);
                       return null;
                   }
                   if (front.length() != back.length()) {
                        log.error("bad key/pair sizes... ");
                       log.error("front = " + front);
                       log.error("back = " + back);
                       return null;
                   }

            sb = sb.append(front).append(StringUtils.reverse(back));
            return new Text(sb.toString());
           }


        public void reduce(Text key, Iterable<Text> values, Context context)
                throws InterruptedException, IOException {

            Text joined = pairJoin(values);
            if (joined != null) {
                context.write(new Text(">" + key.toString()), joined);
            }

       }
    }

    public static class GraphEdgeMapper
            extends Mapper<Text, Sequence, Text, ReadNode> {

        Logger log = Logger.getLogger(GraphEdgeMapper.class);

        public void map(Text key, Sequence value, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString().trim();

            String sequence = value.seqString();

            int windowSize = context.getConfiguration().getInt("dereplicate.windowsize", 16);
            int editDistance = 1;

            try {
                context.setStatus("generating hash");
                String sequenceHashValue = sequence.substring(0, windowSize) + StringUtils.reverse(sequence).substring(0,windowSize);
                //if (sequenceHashValue.contains("n")) return;
                ReadNode rn = new ReadNode(keyStr, sequenceHashValue, sequence);

                log.info("real value: " + sequenceHashValue + "/" + rn.toString());
                context.write(new Text(sequenceHashValue), rn);
                //context.write(new Text(sequenceHashValue), new ReadNode("", sequenceHashValue, ""));  // cache
                context.setStatus("generating neighbors");
                Set<String> allNeighbors =  generateAllNeighbors(sequenceHashValue, editDistance, new HashSet());
                context.setStatus("writing output:");
                for (String neighborHashValue : allNeighbors) {
//                    log.info("neighbors of " + sequenceHashValue);
//                    log.info("k/v = " + neighborHashValue + "/" + rn.toString());
                    context.write(new Text(neighborHashValue), rn);
                }
//                log.info("");
            }catch (Exception e) {
                log.error("error sequence not correct: id = " + keyStr + " sequence = " + sequence + " \n original message: " + e);
            }


        }

    }

    public static class GraphEdgeMapper2
            extends Mapper<LongWritable, Text, Text, ReadNode> {

        Logger log = Logger.getLogger(GraphEdgeMapper.class);

        public void map(LongWritable linenum, Text line, Context context) throws IOException, InterruptedException {

           String[] lineArray = line.toString().split("\t");
           ReadNode r = new ReadNode(lineArray[0]);
           String groupId = lineArray[1];
           String groupHash = lineArray[1].split("\\.")[2];

            context.write(new Text(groupHash), r);

            String sequence = r.sequence;

            int windowSize = context.getConfiguration().getInt("dereplicate.windowsize", 16);
            int editDistance = 1;

            try {
                context.setStatus("generating hash");
                String sequenceHashValue = sequence.substring(0, windowSize) + StringUtils.reverse(sequence).substring(0,windowSize);

                Set<String> allNeighbors =  generateAllNeighbors(sequenceHashValue, editDistance, new HashSet());

                for (String neighborHashValue : allNeighbors) {
                    context.write(new Text(groupHash), r);
                }
            }catch (Exception e) {
                log.error("error sequence not correct: id = " + r + " sequence = " + sequence + " \n original message: " + e);
            }
        }

    }
        private static Set<String> generateAllNeighbors(String start, int distance, Set x) {

            char[] bases = {'a', 't', 'g', 'c', 'n'};
            Set<String> s = new HashSet<String>();

            //s.add(start);
            if (distance == 0) {
                return s;
            }

            for (int i = 0; i < start.length(); i++) {

                for (char basePair : bases) {
                    if (start.charAt(i) == basePair) continue;
                    String n = stringReplaceIth(start, i, basePair);
                    if (x.contains(n)) continue;

                    s.add(n);
                    s.addAll(generateAllNeighbors(n, distance-1, s));
                }

            }

            return s;
        }

        private static String stringReplaceIth(String s, int i, char c) {
            
            return s.substring(0,i) + c + s.substring(i+1);

        }


    /**
     * simple reducer that just outputs the matches grouped by gene
     */
    public static class GraphEdgeReducer extends Reducer<Text, ReadNode, ReadNode, Text> {

        Logger log = Logger.getLogger(GraphEdgeReducer.class);

        public void reduce(Text key, Iterable<ReadNode> values, Context context)
                throws InterruptedException, IOException {

            context.setStatus("parsing readnodes");
            ReadNodeSet rns = new ReadNodeSet(values);
            String keyStr = key.toString();

            context.setStatus("looking for hash in readnodeset with size " + rns.length);
//            log.info("examining node: " + keyStr);            
            if (rns.findHash(keyStr)) {
                log.info("examining node: " + keyStr + " found!");
                context.setStatus("writing output with size " + rns.length);
                int i = 0;
                String cname = rns.canonicalName(keyStr);
                for (ReadNode r : rns.s) {
                    i++;
                    context.setStatus("ReadNode " + i + " in " + cname);
                    context.write(r, new Text(cname));
                }
                log.info("size = " + i);
            }
        }
    }


    public static class AggregateMapper
            extends Mapper<LongWritable, Text, ReadNode, Text> {

        Logger log = Logger.getLogger(AggregateMapper.class);

        public void map(LongWritable count, Text line, Context context) throws IOException, InterruptedException {

            String[] lineArray = line.toString().split("\t");
            ReadNode r = new ReadNode(lineArray[0]);
            String[] groupInfo = lineArray[1].split(("\\."));

            context.write(r, new Text(groupInfo[0]+"."+groupInfo[1]));
        }
}

    public static class AggregateReducer extends Reducer<ReadNode, Text, ReadNode, Text> {

        Logger log = Logger.getLogger(AggregateReducer.class);

        public void reduce(ReadNode key, Iterable<Text> values, Context context)
                throws InterruptedException, IOException {

            int max = 0;
            Text rsMax = null;
            for (Text rs : values) {
                String[] groupidComponents= rs.toString().split("\\.");
                int groupSize = Integer.parseInt(groupidComponents[0]);
                if (groupSize > max) {
                    max = groupSize;
                    rsMax = new Text(rs);
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

        public void reduce(Text key, Iterable<ReadNode> values, Context context)
                throws InterruptedException, IOException {

            // determine consensus sequence and output fasta formatted file
            // key is the group canonical name
            // values are the set of read nodes

            ReadNodeSet rs = new ReadNodeSet(values);
            String consensus;
            try {
                consensus =  rs.fastaConsensusSequence();
            } catch (Exception e) {
                log.info("error generating consensus: key = " + key);
                log.info(e);
                return;
            }
            context.write(new Text(rs.fastaHeader() + " numberOfReads=" + rs.s.size()), new Text(consensus));
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


        int iterationNum = 0;
        int editDistance = conf.getInt("dereplicate.editdistance", 1);

        FileSystem fs = FileSystem.get(conf);
        boolean recalculate = false;

        if (!fs.exists(new Path(otherArgs[1]+"/step0"))) {
            recalculate = true;

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
        }

        if (recalculate) {
            fs.delete(new Path(otherArgs[1]+"/step1"), true);
        }
        if (!fs.exists(new Path(otherArgs[1]+"/step1"))) {

            do {
                recalculate = true;

                Job job = new Job(conf, "dereplicate-step1");

                job.setJarByClass(Dereplicate.class);
                if (iterationNum == 0) {
                    job.setInputFormatClass(FastaInputFormat.class);
                    job.setMapperClass(GraphEdgeMapper.class);
                } else {
                    job.setInputFormatClass(TextInputFormat.class);
                    job.setMapperClass(GraphEdgeMapper2.class);
                }

                job.setReducerClass(GraphEdgeReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(ReadNode.class);
                job.setNumReduceTasks(conf.getInt("dereplicate.numreducers", 1));

                if (iterationNum == 0)
                    FileInputFormat.addInputPath(job, new Path(otherArgs[1]+"/step0"));
                else
                    FileInputFormat.addInputPath(job, new Path(otherArgs[1]+"/step1/iteration" + (iterationNum-1)));

                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"/step1/iteration" + iterationNum));

                job.waitForCompletion(true);

                iterationNum++;
                
            } while (iterationNum < editDistance);
        }

        /*
        now setup groups
         */

        if (recalculate) {
            fs.delete(new Path(otherArgs[1]+"/step2"), true);
        }
        if (!fs.exists(new Path(otherArgs[1]+"/step2"))) {
            recalculate = true;

            Job job2 = new Job(conf, "dereplicate-step2");

            job2.setJarByClass(Dereplicate.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setMapperClass(AggregateMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job2.setReducerClass(AggregateReducer.class);
            job2.setOutputKeyClass(ReadNode.class);
            job2.setOutputValueClass(Text.class);
            job2.setNumReduceTasks(conf.getInt("dereplicate.numreducers", 1));

            FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"/step1/iteration" + (iterationNum-1)));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/step2"));


            job2.waitForCompletion(true);
        }

        /*
        now setup groups
         */

        if (recalculate) {
            fs.delete(new Path(otherArgs[1]+"/step3"), true);
        }
     if (!fs.exists(new Path(otherArgs[1]+"/step3"))) {
         recalculate = true;

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
}



