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

package gov.jgi.meta.exec;


import gov.jgi.meta.MetaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;


/**
 * class that cleans up the temp directories specified in the config file
 *
 */
public class TempCleanerCommand implements CommandLineProgram {

   final String DEFAULTTMPDIR = "/tmp";
	
   Logger log = Logger.getLogger(TempCleanerCommand.class );   

   String cleanup_commandLine = null;
   
   String tmpDir     = null;

   String stdout = null;
   String stderr = null;
   int exitValue = 0;
   
   Boolean doCleanup = true;


   /**
    * new blast command based on default parameters
    * @throws IOException if tmp working directory can't be created
    */
   public TempCleanerCommand() throws IOException
   {
      log.info("initializing");
      tmpDir     = DEFAULTTMPDIR;
   }

   public TempCleanerCommand(Configuration config) throws IOException
   {
      log.info("initializing");
      String c;

      if ((c = config.get("cleaner.commandline")) != null)
      {
    	  cleanup_commandLine = c;
      }
      else
      {
    	  cleanup_commandLine = "/bin/sh rm -rf ";
      }

      if ((c = config.get("assembler.tmpdir")) != null)
      {
         tmpDir = c;
      }
      else
      {
         tmpDir = DEFAULTTMPDIR;
      }

      doCleanup = config.getBoolean("assembler.cleanup", true);

   }

   protected void finalize() throws Throwable
   {
      super.finalize();
   }

   public void cleanup()
   {
      log.info("Cleanup Task: cleaning up " + tmpDir);
     if (tmpDir != null)
     {
        Boolean r;
        r = MetaUtils.recursiveDelete(new File(tmpDir));
        if (!r) { log.info("unable to delete " + tmpDir); }
     }
   }

   /**
    * cleanup temp space
    *
    * @param seqDatabase is the key/value map of sequences that act as reference keyed by name
    * @return a list of sequence ids in the reference that match the cazy database
    */
   public Map<String, String> exec(String groupId, Map<String, String> seqDatabase, Reducer.Context context)  throws IOException, InterruptedException
   {
      Map<String, String> s = new HashMap<String, String>();

      cleanup();
      
      return(s);
   }
}
