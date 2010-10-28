/*
 * Copyright (c) 2010, The Regents of the University of California, through Lawrence Berkeley
 * National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy).
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * (1) Redistributions of source code must retain the above copyright notice, this list of conditions and the
 * following disclaimer.
 *
 * (2) Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 * and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * (3) Neither the name of the University of California, Lawrence Berkeley National Laboratory, U.S. Dept.
 * of Energy, nor the names of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You are under no obligation whatsoever to provide any bug fixes, patches, or upgrades to the
 * features, functionality or performance of the source code ("Enhancements") to anyone; however,
 * if you choose to make your Enhancements available either publicly, or directly to Lawrence Berkeley
 * National Laboratory, without imposing a separate written license agreement for such Enhancements,
 * then you hereby grant the following license: a  non-exclusive, royalty-free perpetual license to install,
 * use, modify, prepare derivative works, incorporate into other computer software, distribute, and
 * sublicense such enhancements or derivative works thereof, in binary and source code form.
 */

package gov.jgi.meta;


import gov.jgi.meta.hadoop.input.FastaBlockLineReader;
import gov.jgi.meta.hadoop.input.FastaInputFormat;
import gov.jgi.meta.hadoop.reduce.AssembleByGroupKey;
import gov.jgi.meta.hadoop.reduce.IdentityReducerGroupByKey;
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
      Logger              log = Logger.getLogger(this.getClass());
      Map<String, String> contigs;
      Map < String, Set < String >> contigKmersFront;
      Map < String, Set < String >> contigKmersRear;
      int kmerSize;
      int contigEndLength;
      int numErrors;
      int maxContigSize;


      /**
       * reads all contigs from given file or directory and indexs them based on their kmers
       *
       * @param contigFileName - the file or directory where the contigs are stored
       * @throws IOException if some file error comes up
       */
      private void readContigs(Context context, String contigFileName) throws IOException
      {
         Configuration conf         = new Configuration();
         FileSystem    fs           = FileSystem.get(conf);
         Path          filenamePath = new Path(contigFileName);

         log.info("reading and indexing the contigs");

         if (!fs.exists(filenamePath))
         {
            throw new IOException("file not found: " + contigFileName);
         }

         contigs          = new HashMap<String, String>();
         contigKmersFront = new HashMap < String, Set < String >> ();
         contigKmersRear  = new HashMap < String, Set < String >> ();

         /*
          * the contigs may be in multiple files (ala hadoop method), so lets
          * find all files in the directory if necessary
          */

         for (Path f : MetaUtils.findAllPaths(filenamePath))
         {
            /*
             * open file and read the contigs
             */
            FSDataInputStream       in         = fs.open(f);
            FastaBlockLineReader    fblr       = new FastaBlockLineReader(in);
            Text                    key        = new Text();
            long                    length     = fs.getFileStatus(f).getLen();
            HashMap<String, String> tmpcontigs = new HashMap<String, String>();


            fblr.readLine(key, tmpcontigs, Integer.MAX_VALUE, (int)length);
            in.close();
            log.info("read " + tmpcontigs.size() + " contigs, indexing with kmersize=" + kmerSize);

            /*
             * add the contigs from this file to the overall total
             */
            contigs.putAll(tmpcontigs);

            /*
             * now index the contigs in this file
             */
            for (String contigName : tmpcontigs.keySet())
            {
               String contigSequence = tmpcontigs.get(contigName);
               int    seqLength      = contigSequence.length();


               if (maxContigSize > 0 && seqLength > maxContigSize) {
                   continue;
               }

               /*
                * process the tail end of contig
                */
               for (int i = Math.max(seqLength - contigEndLength, 0); i <= seqLength - kmerSize; i++)
               {
                  addContigToKmerIndex(contigKmersRear,
                                       MetaUtils.generateAllNeighbors2(contigSequence.substring(i, i + kmerSize), numErrors),
                                       contigName);
               }

               /*
                * now process the front
                */
               for (int i = 0; i <= Math.min(contigEndLength, seqLength) - kmerSize; i++)
               {
                  addContigToKmerIndex(contigKmersFront,
                                       MetaUtils.generateAllNeighbors2(contigSequence.substring(i, i + kmerSize), numErrors),
                                       contigName);
               }
            }
            log.info("done indexing");
         }
         //context.setStatus("calculating neighbors");

         /*
          * now do the neighbor calculation
          */

         if (0 > 0)
         {

            int n = contigKmersFront.size();
            String[] contigKmersArray = contigKmersFront.keySet().toArray(new String[n]);
            context.setStatus("pairwise comparison of " + n + "kmers");

            for (int i = 0; i < n; i++)
            {
               String kmer1 = contigKmersArray[i];

               for (int j = 0; j < i; j++)
               {
                  String kmer2 = contigKmersArray[j];

                  if (calculateHammingDistance(kmer1, kmer2) < numErrors)
                  {
                     Set<String> kmer1Set = new HashSet<String>(contigKmersFront.get(kmer1));

                     contigKmersFront.get(kmer1).addAll(contigKmersFront.get(kmer2));
                     contigKmersFront.get(kmer2).addAll(kmer1Set);
                  }
               }
            }

            n = contigKmersRear.size();
            contigKmersArray = contigKmersRear.keySet().toArray(new String[n]);
            context.setStatus("2: pairwise comparison of " + n + "kmers");
             
            for (int i = 0; i < n; i++)
            {
               String kmer1 = contigKmersArray[i];

               for (int j = 0; j < i; j++)
               {
                  String kmer2 = contigKmersArray[j];

                  if (calculateHammingDistance(kmer1, kmer2) < numErrors)
                  {
                     Set<String> kmer1Set = new HashSet<String>(contigKmersRear.get(kmer1));

                     contigKmersRear.get(kmer1).addAll(contigKmersRear.get(kmer2));
                     contigKmersRear.get(kmer2).addAll(kmer1Set);
                  }
               }
            }
         }
      }


      private static int calculateHammingDistance(String s1, String s2)
      {
         int sum = 0;

         for (int i = 0; i < s1.length(); i++)
         {
            if (s1.charAt(i) != s2.charAt(i)) { sum++; }
         }

         return(sum);
      }


      private static void addContigToKmerIndex(Map < String, Set < String >> index, Set<String> kmerSet, String contigName)
      {
         for (String kmer : kmerSet)
         {
            addContigToKmerIndex(index, kmer, contigName);
         }
      }


      private static void addContigToKmerIndex(Map < String, Set < String >> index, String kmer, String contigName)
      {
         if (index.containsKey(kmer))
         {
            index.get(kmer).add(contigName);
         }
         else
         {
            HashSet<String> l = new HashSet<String>();
            l.add(contigName);
            index.put(kmer, l);
         }
      }


      /**
       * initialize the map process by reading the contigs and execution parameters
       *
       * @param context is the map execution context by hadoop
       * @throws IOException          if any file related error occures
       * @throws InterruptedException
       */
      protected void setup(Context context)       throws IOException, InterruptedException
      {
         log.info("running map-setup");

         /*
          * read parameters from the map context
          */
         String contigFileName = context.getConfiguration().get("contigfilename");
         kmerSize        = context.getConfiguration().getInt("kmersize", 50);
         contigEndLength = context.getConfiguration().getInt("contigendlength", 100);
         numErrors       = context.getConfiguration().getInt("numerrors", 0);
         maxContigSize   = context.getConfiguration().getInt("maxcontigsize", 0);
          
         /*
          * read and index the contigs
          */
         readContigs(context, contigFileName);
         log.info("done with setup");
      }


      public void map(Text seqid, Sequence s, Context context) throws IOException, InterruptedException
      {
         String   sequence = s.seqString();
         Text     seqText  = new Text(seqid.toString() + "&" + sequence);
         ReadNode rn       = new ReadNode(seqid.toString(), "", sequence);

         if (!sequence.matches("[atgcn]*"))
         {
            log.error("sequence " + seqid + " is not well formed: " + sequence);
            return;
         }

         /*
          * generate kmers
          */
         int         seqsize = sequence.length();
         Set<String> l       = new HashSet<String>();

         /*
          * first do the front
          */
         for (int i = 0; i <= Math.min(1, seqsize - kmerSize); i++)
         {
            String kmer = sequence.substring(i, i + kmerSize);
            l.addAll(findMatch(contigKmersRear, kmer, numErrors));
         }

         /*
          * now the back
          */
         for (int i = Math.max(0, seqsize - kmerSize - 1); i <= seqsize - kmerSize; i++)
         {
            String kmer = sequence.substring(i, i + kmerSize);
            l.addAll(findMatch(contigKmersFront, kmer, numErrors));
         }

         /*
          * now do the same with the reverse complement
          */
         String sequenceComplement = MetaUtils.reverseComplement(sequence);

         for (int i = 0; i <= Math.min(1, seqsize - kmerSize); i++)
         {
            String kmer = sequenceComplement.substring(i, i + kmerSize);
            l.addAll(findMatch(contigKmersFront, kmer, numErrors));
         }

         /*
          * now the back
          */
         for (int i = Math.max(0, seqsize - kmerSize - 1); i <= seqsize - kmerSize; i++)
         {
            String kmer = sequence.substring(i, i + kmerSize);
            l.addAll(findMatch(contigKmersRear, kmer, numErrors));
         }

         /*
          * finally, output all the contigs that match this sequence.
          */
         if (l.size() > 0) {
            context.getCounter("contigkmer", "NUMBER_OF_READS_THAT_HIT_ANY_CONTIGS").increment(1);
         }

         for (String contigMatch : l)
         {
            context.write(new Text(contigMatch), new Text(rn.id + "&" + rn.sequence));
            context.write(new Text(contigMatch), new Text(contigMatch + "&" + contigs.get(contigMatch)));
         }
      }


      private Set<String> findMatch(Map < String, Set < String >> index, String kmer, int distance)
      {
          Set<String> kmerSet = MetaUtils.generateAllNeighbors2(kmer, distance);
          Set<String> contigSet = new HashSet<String>();

          for (String k : kmerSet) {
              if (index.get(k) != null) contigSet.addAll(index.get(k));
          }
         return contigSet;
      }
   }


   static boolean iterationAlreadyComplete(String outputDirectoryName, int iterationNumber) throws IOException
   {
      // if outputDirectoryName exists and ../contigs-step1.fas file exists

      Configuration conf = new Configuration();
      FileSystem    fs   = FileSystem.get(conf);
      Path          outputDirectoryPath = new Path(outputDirectoryName + "/step" + iterationNumber);

      //Path resultFilePath = new Path(outputDirectoryName+"/contigs-"+iterationNumber+".fas");

      return(fs.exists(outputDirectoryPath));    // && fs.exists(resultFilePath));
   }


   /**
    * starts off the hadoop application
    *
    * @param args specify input file cassandra host and kmer size
    * @throws Exception
    */
   public static void main(String[] args) throws Exception
   {
      Logger log = Logger.getLogger(ContigKmer.class );

      /*
       * load the application configuration parameters (from deployment directory)
       */

      Configuration conf = new Configuration();

      String[] otherArgs = MetaUtils.loadConfiguration(conf, args);

      /*
       * process arguments
       */
      if ((otherArgs.length < 3) || (otherArgs.length > 4))
      {
         System.err.println("Usage: contigkmer <contigfile> <readfile> <outputdir> <numiterations optional>");
         System.exit(2);
      }


      int numberOfIterations = 1;

      if (otherArgs.length == 4)
      {
         numberOfIterations = Integer.parseInt(otherArgs[3]);
      }


      /*
       * seems to help in file i/o performance
       */
      conf.setInt("io.file.buffer.size", 1024 * 1024);

      log.info(System.getProperty("application.name") + "[version " + System.getProperty("application.version") + "] starting with following parameters");
      log.info("\tsequence file: " + otherArgs[1]);
      log.info("\tcontig dir: " + otherArgs[0]);

      String[] optionalProperties =
      {
         "mapred.min.split.size",
         "mapred.max.split.size",
         "contigkmer.numreducers",
         "contigkmer.sleep",
         "kmersize",
         "numerrors",
         "contigendlength",
         "assembler.readsizelimit",
         "assembler.removeidenticalsequences",
         "assembler.filterbysizespecial",
         "assembler.command",
         "cap3.commandline",
         "cap3.commandpath",
      };

      MetaUtils.printConfiguration(conf, log, optionalProperties);

      int sleep      = conf.getInt("contigkmer.sleep", 60000);
      int iteration  = 0;
      int numContigs = 0;

      String              inputContigsFileOrDir = otherArgs[0];
      Map<String, String> results = new TreeMap<String, String>();
      results.putAll(MetaUtils.readSequences(otherArgs[0]));


      do
      {
         iteration++;
         System.out.println(" *******   iteration " + iteration + "   ********");

         // check to see if output already exists.

         String outputContigFileName = otherArgs[2] + "/" + "contigs-" + iteration + ".fas";
         String outputContigDirName  = otherArgs[2] + "/" + "step" + iteration;

         Boolean calculationDonePreviously = iterationAlreadyComplete(otherArgs[2], iteration);

         if (!calculationDonePreviously)
         {
            conf.set("contigfilename", inputContigsFileOrDir);

            Job job0 = new Job(conf, "configkmer: " + "iteration " + iteration + ", file = " + inputContigsFileOrDir);
            job0.setJarByClass(ContigKmer.class );
            job0.setInputFormatClass(FastaInputFormat.class );
            job0.setMapperClass(ContigKmerMapper.class );
            //job.setCombinerClass(IntSumReducer.class);
            job0.setReducerClass(AssembleByGroupKey.class );
            //job0.setReducerClass(IdentityReducerGroupByKey.class);
            job0.setOutputKeyClass(Text.class );
            job0.setOutputValueClass(Text.class );
            job0.setNumReduceTasks(conf.getInt("contigkmer.numreducers", 1));

            FileInputFormat.addInputPath(job0, new Path(otherArgs[1]));      // this is the reads file
            FileOutputFormat.setOutputPath(job0, new Path(outputContigDirName));

            job0.waitForCompletion(true);
         }
         else
         {
            System.out.println("Found previous results ... skipping iteration " + iteration);
         }

         numContigs = MetaUtils.countSequences(outputContigDirName);
         Map<String, String> tmpresults = MetaUtils.readSequences(outputContigDirName);
         for (String k : tmpresults.keySet())
         {
            String[] a = k.split("-", 2);
            results.put(a[0], tmpresults.get(k));
         }

         try {
            MetaUtils.sequenceToFile(results, outputContigFileName);
         }
         catch (IOException e) {
            System.out.println(e);
            System.out.println("continuing");
         }

         inputContigsFileOrDir = outputContigDirName;
      } while (iteration < numberOfIterations && numContigs > 0);
   }
}
