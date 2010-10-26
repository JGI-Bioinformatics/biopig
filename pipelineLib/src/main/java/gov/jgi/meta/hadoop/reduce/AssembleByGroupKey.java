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

package gov.jgi.meta.hadoop.reduce;

import gov.jgi.meta.exec.CapCommand;
import gov.jgi.meta.exec.CommandLineProgram;
import gov.jgi.meta.exec.VelvetCommand;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * simple reducer that just outputs the matches grouped by gene
 */
public class AssembleByGroupKey extends Reducer<Text, Text, Text, Text> {
   private IntWritable result = new IntWritable();

   static Logger log = Logger.getLogger(AssembleByGroupKey.class );

   /**
    * blast command wrapper
    */
   CommandLineProgram assemblerCmd = null;

   Boolean assemblerKeepTopHitOnly           = false;
   Boolean assemblerFilterBySizeSpecial      = false;
   Boolean assemblerRemoveIdenticalSequences = false;
   int     assemblerLimitReadsToSize         = 0;


   /**
    * initialization of mapper retrieves connection parameters from context and opens socket
    * to cassandra data server
    *
    * @param context is the hadoop reducer context
    */
   protected void setup(Context context) throws IOException
   {
      log.info("initializing reducer class for job: " + context.getJobName());
      log.info("\tinitializing reducer on host: " + InetAddress.getLocalHost().getHostName());

      String assembler = context.getConfiguration().get("assembler.command", "velvet");
      assemblerKeepTopHitOnly           = context.getConfiguration().getBoolean("assembler.keeptophit", false);
      assemblerFilterBySizeSpecial      = context.getConfiguration().getBoolean("assembler.filterbysizespecial", false);
      assemblerRemoveIdenticalSequences = context.getConfiguration().getBoolean("assembler.removeidenticalsequences", false);
      assemblerLimitReadsToSize         = context.getConfiguration().getInt("assembler.readsizelimit", 0);

      if ("cap3".equals(assembler))
      {
         assemblerCmd = new CapCommand(context.getConfiguration());
      }
      else if ("velvet".equals(assembler))
      {
         assemblerCmd = new VelvetCommand(context.getConfiguration());
      }
      else
      {
         throw new IOException("no assembler command provided");
      }
       log.info("done with setup");
   }


    protected void finalize() throws Throwable {
         /*
         delete the tmp files if they exist
          */
         cleanup(null);

         super.finalize();
     }


   /**
    * free resource after mapper has finished, ie close socket to cassandra server
    *
    * @param context the reducer context
    */
   protected void cleanup(Context context)
   {
       log.info("cleaning up");
      if (assemblerCmd != null) {
          assemblerCmd.cleanup();
          assemblerCmd = null;
      }
   }

   /**
    * main reduce step, simply string concatenates all values of a particular key with tab as seperator
    */
   public void reduce(Text key, Iterable<Text> values, Context context)
   throws InterruptedException, IOException
   {
//            Text reads = new Text();

//            context.getCounter(AssemblyCounters.NUMBER_OF_GROUPS).increment(1);

      log.info("running reducer class for job: " + context.getJobName());
      log.info("\trunning reducer on host: " + InetAddress.getLocalHost().getHostName());

      /*
       * execute the blast command
       */
      String groupId         = key.toString();
      String specialID       = null;
      String specialSequence = null;

      Map<String, String> s   = null;
      Map<String, String> map = new HashMap<String, String>();

      int count = 0;
      for (Text r : values)
      {
         count++;

         String[] a = r.toString().split("&", 2);

         if (a[0].equals(groupId))
         {
            // this sequence is special
            specialID       = a[0];
            specialSequence = a[1];
            map.put(a[0], a[1]);
            continue;
         }

         if (assemblerRemoveIdenticalSequences && map.containsValue(a[1]))
         {
            continue;
         }

         if (assemblerLimitReadsToSize > 0)
         {
            if (map.size() < assemblerLimitReadsToSize)
            {
               map.put(a[0], a[1]);
            }
         }
         else
         {
            map.put(a[0], a[1]);
         }
      }

      context.getCounter("reduce.assembly", "NUMBER_OF_READS_IN_GROUP").increment(map.size());
      context.getCounter("reduce", "NUMBER_OF_UNIQUE_GROUPS").increment(1);

      Map<String, String> tmpmap = assemblerCmd.exec(groupId, map, context);

      if (tmpmap.size() == 0) {
         
         log.info("assembler retrieved 0 results");
         return;

      }

      ValueComparator     bvc    = new ValueComparator(tmpmap);
      s = new TreeMap<String, String>(bvc);
      s.putAll(tmpmap);


      /*
       * assember must have been successful
       */
      //context.getCounter(AssemblyCounters.NUMBER_OF_SUCCESSFUL_BLATCOMMANDS).increment(1);
      //context.getCounter(AssemblyCounters.NUMBER_OF_MATCHED_READS).increment(s.size());

      log.info("assembler retrieved " + s.size() + " results");
      context.getCounter("reduce.assembly", "NUMBER_OF_CONTIGS_ASSEMBLED").increment(s.size());


      for (String k : s.keySet())
      {
         if (assemblerFilterBySizeSpecial && (specialSequence != null) && (s.get(k).length() <= specialSequence.length())) { continue; }

         context.write(new Text(">" + groupId + "-" + k + " numberOfReadsInput=" + count +
                                ((assemblerLimitReadsToSize > 0) && (assemblerLimitReadsToSize < count)  ? " limit=" + assemblerLimitReadsToSize : " ")),
                       new Text("\n" + s.get(k)));

         if (assemblerKeepTopHitOnly) { break; }
      }

      context.setStatus("Completed");
   }

   class ValueComparator implements Comparator {
      Map base;

      public ValueComparator(Map base)
      {
         this.base = base;
      }

      public int compare(Object a, Object b)
      {
         if (((String)base.get(a)).length() < ((String)base.get(b)).length())
         {
            return(1);
         }
         else if (((String)base.get(a)).length() == ((String)base.get(b)).length())
         {
            return(0);
         }
         else
         {
            return(-1);
         }
      }
   }
}
