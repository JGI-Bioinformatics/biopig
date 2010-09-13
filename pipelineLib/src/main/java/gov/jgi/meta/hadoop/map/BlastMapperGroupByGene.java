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

import gov.jgi.meta.MetaUtils;
import gov.jgi.meta.exec.BlastCommand;
import gov.jgi.meta.exec.BlatCommand;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class BlastMapperGroupByGene
extends Mapper<Object, Map<String, String>, Text, Text> {
   static Logger log            = Logger.getLogger(BlastMapperGroupByGene.class );
   BlastCommand  blastCmd       = null;
   BlatCommand   blatCmd        = null;
   String        geneDBFilePath = null;
   Boolean       isPaired       = true;

   protected void setup(Context context) throws IOException, InterruptedException
   {
      //MetaUtils.configureLog4j();
      if (context.getConfiguration().getBoolean("debug", false)) {
          System.out.println("setting logger to debug");
          log.setLevel(Level.DEBUG);
      }

      blastCmd = new BlastCommand(context.getConfiguration());
      blatCmd  = new BlatCommand(context.getConfiguration());

      geneDBFilePath = context.getConfiguration().get("blast.genedbfilepath");
      isPaired       = context.getConfiguration().getBoolean("blast.readsarepaired", true);

      if ((blastCmd == null) || (blatCmd == null))
      {
         throw new IOException("unable to create commandline executables");
      }

      /*
       * test the existance of the genedbfile
       */
      FileSystem fs           = FileSystem.get(context.getConfiguration());
      Path       filenamePath = new Path(geneDBFilePath);
      if (!fs.exists(filenamePath) || !fs.isFile(filenamePath))
      {
         throw new IOException("file (" + geneDBFilePath + ") does not exist or is not a regular file");
      }


   }


   protected void cleanup(Context context) throws IOException
   {
      if (blastCmd != null) { blastCmd.cleanup(); }
      if (blatCmd != null) { blatCmd.cleanup(); }
      geneDBFilePath = null;
   }


   public void map(Object key, Map<String, String> value, Context context) throws IOException, InterruptedException
   {
      context.getCounter("map", "NUMBER_OF_READS").increment(value.size());

      /*
       * first the blast
       */
      Set<String> s = null;

      try {
         s = blastCmd.exec(value, geneDBFilePath);
      }
      catch (Exception e) {
         /*
          * something bad happened.  update the counter and throw exception
          */
         log.error(e);
         context.getCounter("blast", "NUMBER_OF_ERROR_BLASTCOMMANDS").increment(1);
         throw new IOException(e);
      }

      /*
       * blast executed but did not return sensible values, throw error.
       */
      if (s == null)
      {
         context.getCounter("blast", "NUMBER_OF_ERROR_BLASTCOMMANDS").increment(1);
         log.error("blast did not execute correctly");
         throw new IOException("blast did not execute properly");
      }

      /*
       * blast must have been successful
       */
      context.getCounter("blast", "NUMBER_OF_SUCCESSFUL_BLASTCOMMANDS").increment(1);
      context.getCounter("blast", "NUMBER_OF_MATCHED_READS_AFTER_BLAST").increment(s.size());

      log.debug("blast retrieved " + s.size() + " results");

      for (String k : s)
      {
         /*
          * blast returns the stdout, line by line.  the output is split by tab and
          * the first column is the id of the gene, second column is the read id
          */
         System.out.println("blast output line = " + k);

         String[] a = k.split("\t");

         /*
          * note that we strip out the readid direction.  that is, we don't care if the
          * read is a forward read (id/1) or backward (id/2).
          */

         if (isPaired)
         {
            context.write(new Text(a[0]), new Text(a[1].split("/")[0]));
         }
         else
         {
            context.write(new Text(a[0]), new Text(a[1]));
         }
      }
   }
}
