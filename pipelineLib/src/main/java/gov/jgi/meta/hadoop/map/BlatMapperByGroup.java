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

package gov.jgi.meta.hadoop.map;

import gov.jgi.meta.exec.BlatCommand;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * map class that maps blast output, to expanded set of reads using
 * blat executable.
 */
public class BlatMapperByGroup
extends Mapper<Object, Map<String, String>, Text, Text> {
   static Logger log = Logger.getLogger(BlatMapperByGroup.class );

   /**
    * blast command wrapper
    */
   BlatCommand blatCmd = null;

   /**
    * initialization of mapper retrieves connection parameters from context and
    * opens connection to datastore
    *
    * @param context is the mapper context for this job
    * @throws java.io.IOException
    * @throws InterruptedException
    */
   protected void setup(Context context)
   throws IOException, InterruptedException
   {
      log.debug("initializing map task for job: " + context.getJobName());
      log.debug("initializing maptask on host: " + InetAddress.getLocalHost().getHostName());

      blatCmd = new BlatCommand(context.getConfiguration());
   }


   /**
    * free resource after mapper has finished, ie close socket to cassandra server
    *
    * @param context is the job context for the map task
    */
   protected void cleanup(Context context) throws IOException
   {
      log.debug("deleting map task for job: " + context.getJobName() + " on host: " + InetAddress.getLocalHost().getHostName());
      if (blatCmd != null)
      {
         blatCmd.cleanup();
      }
   }


   /**
    * the map function processes a block of fasta reads through the blast program
    *
    * @param key     - unused (just junk)
    * @param value   - a map of <readid, sequence>'s
    * @param context - configuration context
    * @throws java.io.IOException
    * @throws InterruptedException
    */
   public void map(Object key, Map<String, String> value, Context context) throws IOException, InterruptedException
   {
      log.debug("map task started for job: " + context.getJobName() + " on host: " + InetAddress.getLocalHost().getHostName());

      String  blastOutputFilePath = context.getConfiguration().get("blat.blastoutputfile");
      Boolean skipExecution       = context.getConfiguration().getBoolean("blat.skipexecution", false);

      context.getCounter("map", "NUMBER_OF_READS").increment(value.size());

      /*
       * execute the blast command
       */
      Set<String> s = null;

      try {
         if (!skipExecution)
         {
            s = blatCmd.exec(value, blastOutputFilePath, context);
         }
         else
         {
            s = new HashSet<String>();
         }
      }
      catch (Exception e) {
         /*
          * something bad happened.  update the counter and throw exception
          */
         log.error(e);
         context.getCounter("map.blat", "NUMBER_OF_ERROR_BLATCOMMANDS").increment(1);
         throw new IOException(e);
      }
      if (s == null)
      {
         log.info("unable to retrieve results of blat execution");
         context.getCounter("map.blat", "NUMBER_OF_ERROR_BLATCOMMANDS").increment(1);
      }

      /*
       * blat must have been successful
       */
      context.getCounter("map.blat", "NUMBER_OF_SUCCESSFUL_BLATCOMMANDS").increment(s.size());

      log.debug("blat retrieved " + s.size() + " results");

      for (String k : s)
      {
         /*
          * blat returns the stdout, line by line.  the output is split by tab and
          * the first column is the id of the gene, second column is the read id
          */
         String[] a = k.split(", ", 2);
         Text groupkey = new Text(a[0]);
         String[] sequences = a[1].split(", ");

         context.getCounter("map.blat", "NUMBER_OF_MATCHED_READS").increment(sequences.length);

         for (String seqid : sequences)
         {
            context.write(groupkey, new Text(seqid + "&" + value.get(seqid)));
         }
      }

      context.setStatus("Completed");
   }
}
