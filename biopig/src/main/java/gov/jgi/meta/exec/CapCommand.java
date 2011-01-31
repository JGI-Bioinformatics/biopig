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

import com.devdaily.system.SystemCommandExecutor;
import gov.jgi.meta.MetaUtils;
import gov.jgi.meta.hadoop.input.FastaBlockLineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;


/**
 * class that wraps execution of commandline cap3 program.
 *
 * Use this by creating a new CapCommand than invoking the
 * exec method.  eg:
 *
 * CapCommand b = new CapCommand();
 * r = b.exec(...)
 *
 */
public class CapCommand implements CommandLineProgram {
   String DEFAULTCOMMANDLINE = "-k 0";
   String DEFAULTCOMMANDPATH = "cap3";
   String DEFAULTTMPDIR      = "/tmp/cap3";

   /**
    * logger
    */
   Logger log = Logger.getLogger(CapCommand.class );

   /**
    * the commandline to execute (all options except the input/output)
    */
   String commandLine = null;

   /**
    * the location of the executable in the filesystem
    */
   String commandPath = null;

   /**
    * temporary directory to use for intermediate files
    */
   String tmpDir     = null;
   File   tmpDirFile = null;

   /**
    * the contents of the stdout after executing the command
    */
   String stdout = null;

   /**
    * the contents of the stderr after executing the command
    */
   String stderr = null;

   /**
    * the shell return value after executing the command
    */
   int exitValue = 0;

   /**
    * flat to determine whether to clean up directories after
    * execution
    */
   Boolean doCleanup = true;

   /**
    * new cap3 command based on default parameters
    * @throws IOException if tmp directories can't be created
    */
   public CapCommand() throws IOException
   {
      commandLine = DEFAULTCOMMANDLINE;
      commandPath = DEFAULTCOMMANDPATH;
      tmpDir      = DEFAULTTMPDIR;
      tmpDirFile  = MetaUtils.createTempDir(tmpDir);
   }

   /**
    * new cap3 command based on values stored in the configuration.
    * <p/>
    * Looks for the following config values: blast.commandline,
    * blast.commandpath, and blast.tmpdir
    *
    * @param config is the hadoop configuration with overriding values
    *               for commandline options and paths
    * @throws IOException if command can't be configured
    */
   public CapCommand(Configuration config) throws IOException
   {
      String c;

      if ((c = config.get("cap3.commandline")) != null)
      {
         commandLine = c;
      }
      else
      {
         commandLine = DEFAULTCOMMANDLINE;
      }
      if ((c = config.get("cap3.commandpath")) != null)
      {
         commandPath = c;
      }
      else
      {
         commandPath = DEFAULTCOMMANDPATH;
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

      /*
       * do sanity check to make sure all paths exist
       */
      //checkFileExists(commandLine);
      //checkFileExists(commandPath);
      //checkDirExists(tmpDir);

      /*
       * if all is good, create a working space inside tmpDir
       */

      tmpDirFile = MetaUtils.createTempDir(tmpDir);
   }

   /**
    * destructor deletes the tmp space if it was created
    *
    * @throws Throwable
    */
   protected void finalize() throws Throwable
   {
      cleanup();
      super.finalize();
   }

   /**
    * clean up all footsteps of program
    */
   public void cleanup()
   {
      log.info("deleting tmp file: " + tmpDirFile.getPath());

      if (this.doCleanup)
      {
         if (tmpDirFile != null)
         {
            MetaUtils.recursiveDelete(tmpDirFile);
            tmpDirFile = null;
         }
      }
   }

   public File getTmpDir()
   {
      return(tmpDirFile);
   }

   /**
    * execute the cap3 command and return a set of contigs that result from the assembly
    *
    * @param seqDatabase is the key/value map of sequences that act as reference keyed by name
    * @return a map of sequence ids in the reference that match the cazy database
    * @throws IOException if file system error occured
    * @throws InterruptedException if execution got interrupted
    */
   public Map<String, String> exec(String groupId, Map<String, String> seqDatabase, Reducer.Context context)
   throws IOException, InterruptedException
   {
      Long executionStartTime = new Date().getTime();

      Map<String, String> s = new HashMap<String, String>();

      log.info("Preparing Assembly execution");
      if (context != null) { context.setStatus("Preparing Assembly execution"); }

      /*
       * dump the database from the map to a file
       */
      String seqFilepath = MetaUtils.sequenceToLocalFile(seqDatabase, tmpDirFile + "/reads.fa");

      if (seqFilepath == null)
      {
         /*
          * return with fail
          */
         throw new IOException("unable to find file: " + tmpDirFile + "/reads.fa");
      }

      if (context != null) { context.setStatus("running cap3 with " + seqDatabase.size() + " reads"); }

      /*
       * now set up a blat execution
       */
      List<String> commands = new ArrayList<String>();
      commands.add("/bin/sh");
      commands.add("-c");
      commands.add(commandPath + " " + seqFilepath + " " + commandLine);

      log.info("command = " + commands);

      SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
      exitValue = commandExecutor.executeCommand();

      // stdout and stderr of the command are returned as StringBuilder objects
      stdout = commandExecutor.getStandardOutputFromCommand().toString();
      stderr = commandExecutor.getStandardErrorFromCommand().toString();

      log.debug("exit = " + exitValue);
      log.debug("stdout = " + stdout);
      log.debug("stderr = " + stderr);

      Long executionTime = new Date().getTime() - executionStartTime;

      if (context != null) { context.getCounter("reduce.assembly", "EXECUTION_TIME").increment(executionTime); }

      /*
       * now parse the output and clean up
       */

      FileInputStream      fstream = null;
      FastaBlockLineReader in      = null;

      try {
         Text t = new Text();
         fstream = new FileInputStream(tmpDirFile.getPath() + "/reads.fa.cap.contigs");
         in      = new FastaBlockLineReader(fstream);
         int bytes = in.readLine(t, s);
      } catch (Exception e) {
         log.error("unable to find outputfile:" + e);
         throw new IOException(e);
      } finally {
         if (fstream != null) { fstream.close(); }
         if (in != null) { in.close(); }
         System.gc();
      }

      return(s);
   }
}

