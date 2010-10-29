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
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;


/**
 * class that wraps execution of commandline velvet program.
 *
 * Use this by creating a new velvetCommand than invoking the
 * exec method.  eg:
 *
 * VelvetCommand v = new VelvetCommand();
 * r = v.exec(...)
 *
 */
public class VelvetCommand implements CommandLineProgram {
   String DEFAULT_VELVETH_COMMANDLINE = "21";
   String DEFAULT_VELVETG_COMMANDLINE = "";
   String DEFAULT_VELVETH_COMMANDPATH = "velveth";
   String DEFAULT_VELVETG_COMMANDPATH = "velvetg";
   String DEFAULTTMPDIR = "/tmp/velvet";

   /**
    * logger
    */
   Logger log = Logger.getLogger(VelvetCommand.class );

   /**
    * the commandline to execute (all options except the input/output)
    */
   String velveth_commandLine = null;
   String velvetg_commandLine = null;

   /**
    * the location of the executable in the filesystem
    */
   String velveth_commandPath = null;
   String velvetg_commandPath = null;

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
    * new blast command based on default parameters
    * @throws IOException if tmp working directory can't be created
    */
   public VelvetCommand() throws IOException
   {
      log.info("initializing");
      // look in configuration file to determine default values
      velveth_commandLine = DEFAULT_VELVETH_COMMANDLINE;
      velvetg_commandLine = DEFAULT_VELVETG_COMMANDLINE;
      velveth_commandPath = DEFAULT_VELVETH_COMMANDPATH;
      velvetg_commandPath = DEFAULT_VELVETG_COMMANDPATH;

      tmpDir     = DEFAULTTMPDIR;
      tmpDirFile = MetaUtils.createTempDir(tmpDir);
   }

   /**
    * new blast command based on values stored in the configuration.
    * <p/>
    * Looks for the following config values:
    * <ul>
    * <li> velveth.commandline </li>
    * <li> velveth.commandpath</li>
    * <li> velvetg.commandline</li>
    * <li> velvetg.commandpath</li>
    * <li> assembler.tmpdir</li>
    * <li> assembler.cleanup</li>
    * </ul>
    *
    * @param config is the hadoop configuration with overriding values
    *               for commandline options and paths
    * @throws IOException if tmp working directory can't be created
    */
   public VelvetCommand(Configuration config) throws IOException
   {
      log.info("initializing");
      String c;

      if ((c = config.get("velveth.commandline")) != null)
      {
         velveth_commandLine = c;
      }
      else
      {
         velveth_commandLine = DEFAULT_VELVETH_COMMANDLINE;
      }
      if ((c = config.get("velveth.commandpath")) != null)
      {
         velveth_commandPath = c;
      }
      else
      {
         velveth_commandPath = DEFAULT_VELVETH_COMMANDPATH;
      }


      if ((c = config.get("velvetg.commandline")) != null)
      {
         velvetg_commandLine = c;
      }
      else
      {
         velvetg_commandLine = DEFAULT_VELVETG_COMMANDLINE;
      }
      if ((c = config.get("velvetg.commandpath")) != null)
      {
         velvetg_commandPath = c;
      }
      else
      {
         velvetg_commandPath = DEFAULT_VELVETG_COMMANDPATH;
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

   public File getTmpDir() { return(tmpDirFile); }

   public void cleanup()
   {
      log.info("cleaning up " + tmpDirFile.getPath());
      if (this.doCleanup)
      {
         if (tmpDirFile != null)
         {
            Boolean r;
            r = MetaUtils.recursiveDelete(new File(tmpDirFile.getPath() + "/sillyDirectory"));
            if (!r) { log.info("unable to delete " + tmpDirFile.getPath() + "/sillyDirectory"); }
            r = MetaUtils.recursiveDelete(tmpDirFile);
            if (!r) { log.info("unable to delete " + tmpDirFile.getPath()); }
            tmpDirFile = null;
         }
      }
   }

   /**
    * execute the blast command and return a list of sequence ids that match
    *
    * @param seqDatabase is the key/value map of sequences that act as reference keyed by name
    * @return a list of sequence ids in the reference that match the cazy database
    */
   public Map<String, String> exec(String groupId, Map<String, String> seqDatabase, Reducer.Context context)  throws IOException, InterruptedException
   {
      Map<String, String> s = new HashMap<String, String>();

      log.info("Preparing Assembly execution using velvet");
      if (context != null) { context.setStatus("Preparing velvet execution"); }

      String seqFilepath = MetaUtils.sequenceToLocalFile(seqDatabase, tmpDirFile.getPath(), "reads.fa");

      if (seqFilepath == null)
      {
         throw new IOException("seqFilepath is null, can't run assembler (velvet)");
      }

      /*
       * first execute the velveth command
       */

      if (context != null) { context.setStatus("Executing velveth"); }

      List<String> commands = new ArrayList<String>();
      commands.add("/bin/sh");
      commands.add("-c");
      commands.add(velveth_commandPath + " " + tmpDirFile.getPath() + "/sillyDirectory " +
                   velveth_commandLine + " " + seqFilepath + " ; " +
                   velvetg_commandPath + " " + tmpDirFile.getPath() + "/sillyDirectory " +
                   velvetg_commandLine + "; chmod 777 " + tmpDirFile.getPath() + "/sillyDirectory");

      log.info("command = " + commands);

      SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
      exitValue = commandExecutor.executeCommand();

      // stdout and stderr of the command are returned as StringBuilder objects
      stdout = commandExecutor.getStandardOutputFromCommand().toString();
      stderr = commandExecutor.getStandardErrorFromCommand().toString();

      log.debug("exit = " + exitValue);
      log.debug("stdout = " + stdout);
      log.debug("stderr = " + stderr);

      if (exitValue != 0)
      {
         System.out.println("velvet output != 0, checking stderr");
         System.out.println("stderr = " + stderr);
         System.out.println("match = " + stderr.indexOf("PreGraph file incomplete"));
         if (stderr.indexOf("PreGraph file incomplete") > -1)
         {
            // special case here.. no results computed, but no error either
            return(s);
         }
         else
         {
            throw new IOException(stderr);
         }
      }

      /*
       *  now parse the output and clean up
       */

      FileInputStream      fstream = null;
      FastaBlockLineReader in      = null;

      try {
         Text t = new Text();
         fstream = new FileInputStream(tmpDirFile.getPath() + "/sillyDirectory/contigs.fa");
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
