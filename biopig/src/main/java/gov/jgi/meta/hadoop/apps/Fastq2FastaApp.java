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

package gov.jgi.meta.hadoop.apps;


import gov.jgi.meta.MetaUtils;
import gov.jgi.meta.hadoop.input.FastaInputFormat;
import gov.jgi.meta.hadoop.input.FastqInputFormat;
import gov.jgi.meta.hadoop.map.FastaIdentityMapper;
import gov.jgi.meta.hadoop.output.FastaOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;


public class Fastq2FastaApp {
   /**
    * starts off the hadoop application
    *
    * @param args specify input file cassandra host and kmer size
    * @throws Exception
    */
   public static void main(String[] args) throws Exception
   {
      Logger log = Logger.getLogger(Fastq2FastaApp.class );

      /*
       * load the application configuration parameters (from deployment directory)
       */

      Configuration conf = new Configuration();

      String[] otherArgs = MetaUtils.loadConfiguration(conf, "fastq2fasta-conf.xml", args);

      /*
       * process arguments
       */
      if ((args.length < 2) || (args.length > 3))
      {
         System.err.println("Usage: fastq2fasta <input> <output>");
         System.exit(2);
      }

      /*
       * seems to help in file i/o performance
       */
      conf.setInt("io.file.buffer.size", 1024 * 1024);

      log.info(System.getProperty("application.name") + "[version " + System.getProperty("application.version") + "] starting with following parameters");
      log.info("\tinput file: " + otherArgs[0]);
      log.info("\toutput dir: " + otherArgs[1]);

      String[] optionalProperties =
      {
         "mapred.min.split.size",
         "mapred.max.split.size",
      };
      System.out.println("no reduce version");

      MetaUtils.printConfiguration(conf, log, optionalProperties);

      Job job0 = new Job(conf, "fastq2fasta");
      job0.setJarByClass(Fastq2FastaApp.class );
      job0.setInputFormatClass(FastaInputFormat.class );
      job0.setMapperClass(FastaIdentityMapper.class );
      job0.setOutputKeyClass(Text.class );
      job0.setOutputValueClass(Text.class );
//       job0.setReducerClass(IdentityReducerGroupByKey.class);
      job0.setOutputFormatClass(FastaOutputFormat.class );
      job0.setNumReduceTasks(0);

      FileInputFormat.addInputPath(job0, new Path(otherArgs[0]));   // this is the reads file
      FileOutputFormat.setOutputPath(job0, new Path(otherArgs[1]));

      job0.waitForCompletion(true);
   }
}
