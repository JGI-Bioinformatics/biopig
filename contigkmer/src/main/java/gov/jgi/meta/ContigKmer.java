/*
 * Copyright (c) 2010, Joint Genome Institute (JGI) United States Department of Energy
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *     must display the following acknowledgement:
 *     This product includes software developed by the JGI.
 * 4. Neither the name of the JGI nor the
 *     names of its contributors may be used to endorse or promote products
 *     derived from this software without specific prior written permission.
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


import gov.jgi.meta.hadoop.input.FastaBlockLineReader;
import gov.jgi.meta.hadoop.input.FastaInputFormat;
import gov.jgi.meta.hadoop.reduce.AssembleByGroupKey;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.biojava.bio.seq.Sequence;

import java.io.*;
import java.net.InetAddress;
import java.util.*;


public class ContigKmer {

    public static class ContigKmerMapper
            extends Mapper<Text, Sequence, Text, Text> {

        Logger log = Logger.getLogger(this.getClass());
        Map<String,String> contigs;
        Map<String,Set<String>> contigKmers;
        int kmerSize;
        int contigEndLength;


        private String reverseComplement (String s) {
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < s.length(); i++) {
                if (s.charAt(i) == 'a') sb.append("t");
                else if (s.charAt(i) == 't') sb.append("a");
                else if (s.charAt(i) == 'g') sb.append("c");
                else if (s.charAt(i) == 'c') sb.append("g");
                else if (s.charAt(i) == 'n') sb.append("n");                
            }
            return sb.reverse().toString();
        }

        private void readContigs(String contigFileName) throws IOException {

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path filenamePath = new Path(contigFileName);

            if (!fs.exists(filenamePath)) {
                throw new IOException("file not found: " + contigFileName);
            }

            FSDataInputStream in = fs.open(filenamePath);
            FastaBlockLineReader fblr = new FastaBlockLineReader(in);

            Text key = new Text();
            long length = fs.getFileStatus(filenamePath).getLen();
            contigs = new HashMap<String,String>();
            fblr.readLine(key, contigs, Integer.MAX_VALUE, (int) length);
            int hashTableSizeEstimate = contigs.size() * (contigEndLength - kmerSize) * 4 ;

            contigKmers = new HashMap<String,Set<String>>(hashTableSizeEstimate);
            in.close();

            int num = 0;
            for (String k : contigs.keySet()) {
                //log.info("processing: " + num++);
                String contigSequence = contigs.get(k);
                int seqLength = contigSequence.length();
                // tail end of contig
                for (int i = Math.max(seqLength - contigEndLength, 0); i <= seqLength-kmerSize; i++ ) {
                    String kmer = contigSequence.substring(i, i + kmerSize);
                    if (contigKmers.containsKey(kmer)) {
                        contigKmers.get(kmer).add(k);
                    } else {
                        HashSet<String> l = new HashSet<String>();
                        l.add(k);
                        contigKmers.put(kmer, l);
                    }
                }
                // front end of sequence
                for (int i = 0; i <= Math.min(contigEndLength,seqLength)-kmerSize; i++ ) {
                    String kmer = contigSequence.substring(i, i + kmerSize);
                    if (contigKmers.containsKey(kmer)) {
                        contigKmers.get(kmer).add(k);
                    } else {
                        HashSet<String> l = new HashSet<String>();
                        l.add(k);
                        contigKmers.put(kmer, l);
                    }
                }
            }
        }

         protected void setup(Context context)
                throws IOException, InterruptedException
        {
            // read contig file and store sequences with kmers
            String contigFileName = context.getConfiguration().get("contigfilename");
            kmerSize = context.getConfiguration().getInt("kmersize", 50);
            contigEndLength = context.getConfiguration().getInt("contigendlength", 100);
            readContigs(contigFileName);
        }

        public void map(Text seqid, Sequence s, Context context) throws IOException, InterruptedException {

            String sequence = s.seqString();
            Text seqText = new Text(seqid.toString() + "&" + sequence);

            ReadNode rn = new ReadNode(seqid.toString(), "", sequence);

            if (!sequence.matches("[atgcn]*")) {
                log.error("sequence " + seqid + " is not well formed: " + sequence);
                return;
            }

            // generate kmers
            int seqsize = sequence.length();
            Set<String> l = new HashSet<String>();

            int i;
            for (i = 0; i <= seqsize - kmerSize; i++) {
                String kmer = sequence.substring(i, i + kmerSize);
                Set<String> ll = contigKmers.get(kmer);
                if (ll != null) l.addAll(ll);
            }
            String sequenceComplement = reverseComplement(sequence);
            for (i = 0; i <= seqsize - kmerSize; i++) {
                String kmer = sequenceComplement.substring(i, i + kmerSize);
                Set<String> ll = contigKmers.get(kmer);
                if (ll != null) l.addAll(ll);
            }
            if (l.size() != 0) {
                for (String contigMatch : l) {
                    context.write(new Text(contigMatch), new Text(rn.id+"&"+rn.sequence));
                }
            }
        }
    }



    public static class ContigKmerReducer extends Reducer<Text, ReadNode, Text, Text> {

        Logger log = Logger.getLogger(this.getClass());

        public void reduce(Text key, Iterable<ReadNode> values, Context context)
                throws InterruptedException, IOException {

            String keyStr = key.toString();

            HashMap<String,ReadNode> hs = new HashMap<String,ReadNode>();
            for (ReadNode v : values) {
                if (hs.containsKey(v.sequence)) {
                    hs.get(v.sequence).count++;
                } else {
                    hs.put(v.sequence,new ReadNode(v));
                }
            }

            for (ReadNode s : hs.values()) {
                context.write(new Text(">"+keyStr+"&"+s.id + " count=" + s.count), new Text("\n" + s.sequence));
            }

       }
    }


    static String findNewFiles(String inputDirectory, String outputDirectory) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(inputDirectory);
        Path outputPath = new Path(outputDirectory);

        if (!fs.exists(inputPath) || !fs.getFileStatus(inputPath).isDir()) {
            throw new IOException("directory not found: " + inputDirectory);
        }

        FileStatus[] fsArray = fs.listStatus(inputPath);
        for (FileStatus file : fsArray) {
            if (file.getPath().getName().endsWith(".lock")) continue;
            String output = outputDirectory+"/"+file.getPath().getName()+".out";
            String lockfile = file.getPath() + ".lock";
            if (!fs.exists(new Path(output)) && !fs.exists(new Path(lockfile))) {
                return file.getPath().getName();
            }
        }
        return null;
    }

    /**
     * starts off the hadoop application
     *
     * @param args specify input file cassandra host and kmer size
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

         Logger log = Logger.getLogger(ContigKmer.class);

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
        conf.addResource("contigkmer-conf.xml");

        /*
        override properties from user's preferences defined in ~/.meta-prefs
         */

        try {
            java.io.FileInputStream fis = new java.io.FileInputStream(new File(System.getenv("HOME") + "/.meta-prefs"));
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
        if (otherArgs.length < 3 || otherArgs.length > 4) {
            System.err.println("Usage: contigkmer <contigdir> <read> <outputdir> <numiterations optional>");
            System.exit(2);
        }


        int numberOfIterations = 1;

        if (otherArgs.length == 4)
            numberOfIterations = Integer.parseInt(otherArgs[3]);

        /*
        seems to help in file i/o performance
         */
        conf.setInt("io.file.buffer.size", 1024 * 1024);

        log.info(System.getenv("application.name") + "[version " + System.getenv("application.version") + "] starting with following parameters");
        log.info("\tsequence file: " + otherArgs[1]);
        log.info("\tcontig dir: " + otherArgs[0]);

        String[] optionalProperties = {
                "mapred.min.split.size",
                "mapred.max.split.size",
                "contigkmer.numreducers",
                "contigkmer.sleep",
                "kmersize",
                "contigendlength"
        };

        for (String option : optionalProperties) {
            if (conf.get(option) != null) {
                log.info("\toption " + option + ":\t" + conf.get(option));
            }
        }
        int sleep = conf.getInt("contigkmer.sleep", 60000);
        int iteration = 0;

        do {
            String newFileName = findNewFiles(otherArgs[0], otherArgs[2]);
            if (newFileName != null) {
                System.out.println(" *******   iteration " + iteration + "   ********");
                iteration++;
                conf.set("contigfilename", otherArgs[0]+"/"+newFileName);

                Job job0 = new Job(conf, "configkmer: " + "iteration " + iteration + ", file = " + newFileName);
                job0.setJarByClass(ContigKmer.class);
                job0.setInputFormatClass(FastaInputFormat.class);
                job0.setMapperClass(ContigKmerMapper.class);
                //job.setCombinerClass(IntSumReducer.class);
                job0.setReducerClass(AssembleByGroupKey.class);
                job0.setOutputKeyClass(Text.class);
                job0.setOutputValueClass(Text.class);
                job0.setNumReduceTasks(conf.getInt("contigkmer.numreducers", 1));

                FileInputFormat.addInputPath(job0, new Path(otherArgs[1]));
                FileOutputFormat.setOutputPath(job0, new Path(otherArgs[2]+"/"+newFileName+".out"));

                job0.waitForCompletion(true);
            } else {
                System.out.println("sleeping ... for " + sleep/1000 + " seconds");
                Thread.sleep(sleep);
            }

        } while (iteration < numberOfIterations);
    }
}