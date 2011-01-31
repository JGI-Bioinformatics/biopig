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
import gov.jgi.meta.hadoop.input.FastaBlockLineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import com.devdaily.system.*;

import java.io.*;
import java.util.*;


/**
 * class that wraps execution of commandline Blast program.
 *
 * Use this by creating a new BlastCommand object, then running exec. IE:
 *    blastCmd = new BlastCommand(context.getConfiguration());
 *    s = blastCmd.exec(value, geneDBFilePath);
 *    blastCmd.cleanUp();
 *
 * value is a MAP<STRING, STRING> of <readids, sequences> and geneDBFilePath
 * is a file path in hdfs.  this object creates local files, then does a
 * system.exec.  stdout is returned as a set of strings representing each
 * line in the output.
 *
 */
public class BlastCommand {
   String DEFAULTCOMMANDLINE = "-m 8 -p tblastn -b 1000000 -a 10";
   String DEFAULTCOMMANDPATH = "/home/asczyrba/src/blast-2.2.20/bin/blastall";
   String DEFAULTTMPDIR      = "/tmp/blast";

   String DEFAULTFORMATDBCOMMANDLINE = "-o T -p F";
   String DEFAULTFORMATDBCOMMANDPATH = "formatdb";    // by default it should be in the path

   // -p program name
   // -m 8 alignment view options - tabular
   // -b 1000000 number of sequences to show alignments of (max number)
   // -a 10 number of processors to use
   // -o output file
   // -d blast database directory where formatdb was run
   // -i input sequence

   /**
    * logger
    */
   Logger log = Logger.getLogger(BlastCommand.class );

   /**
    * the commandline to execute (all options except the input/output)
    */
   String commandLine = DEFAULTCOMMANDLINE;

   /**
    * the location of the executable in the filesystem
    */
   String commandPath = DEFAULTCOMMANDPATH;

   String formatdbCommandLine = DEFAULTFORMATDBCOMMANDLINE;
   String formatdbCommandPath = DEFAULTFORMATDBCOMMANDPATH;

   /**
    * temporary directory to use for intermediate files
    */
   String tmpDir     = DEFAULTTMPDIR;
   File   tmpDirFile = null;


   public StringBuffer commandString = null;

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
    * flag to leave working directories along (if false) or to remove
    * them after execution (if true)
    */
   Boolean docleanup = true;

   /**
    * the effective blast database size
    */
   long effectiveSize = 0;

   long databasesize = 0;

   Boolean useScaledEValue = false;
   Boolean useEffectiveSize = false;
   float useEValue = 10;

   /**
    * new blast command based on default parameters
    */
   public BlastCommand() throws IOException
   {
      tmpDirFile = MetaUtils.createTempDir(tmpDir);
   }


   /**
    * new blast command based on values stored in the configuration.
    * <p/>
    * Looks for the following config values: blast.commandline,
    * blast.commandpath, and blast.tmpdir, blast.cleanup
    *
    * @param config is the hadoop configuration with overriding values
    *               for commandline options and paths
    * @throws IOException if executable can not be found
    */
   public BlastCommand(Configuration config) throws IOException
   {
       log.info("initializing");
      String c;

      log.info("initializing new blast command");

      if ((c = config.get("blast.commandline")) != null)
      {
         commandLine = c;
      }
      if ((c = config.get("blast.commandpath")) != null)
      {
         commandPath = c;
      }
      if ((c = config.get("formatdb.commandline")) != null)
      {
         formatdbCommandLine = c;
      }
      if ((c = config.get("formatdb.commandpath")) != null)
      {
         formatdbCommandPath = c;
      }

      if ((c = config.get("blast.tmpdir")) != null)
      {
         tmpDir = c;
      }

      docleanup     = config.getBoolean("blast.cleanup", true);

      effectiveSize = config.getLong("blast.effectivedatabasesize", 0);
      useScaledEValue = config.getBoolean("blast.usescaledevalue", false);
      useEffectiveSize = config.getBoolean("blast.useeffectivesize", false);
      useEValue = config.getFloat("blast.useevalue", 10F);

      /*
       * do sanity check to make sure all paths exist
       */
      checkFileExists(commandLine);
      checkFileExists(commandPath);
      checkDirExists(tmpDir);

      /*
       * if all is good, create a working space inside tmpDir
       */

      tmpDirFile = MetaUtils.createTempDir(tmpDir);

       log.info("done initializing: tmp dir = " + tmpDirFile);
   }

   public File getTmpDir () { return tmpDirFile; }

   private int checkFileExists(String filePath) throws IOException
   {
      // TODO: fill out this function
      return(0);
   }


   private int checkDirExists(String filePath) throws IOException
   {
      // TODO: fill out this function
      return(0);
   }


   /**
    * destructor deletes the tmp space if it was created
    *
    * @throws Throwable
    */
   protected void finalize() throws Throwable
   {
      /*
       * delete the tmp files if they exist
       */
      this.cleanup();

      super.finalize();
   }


   /**
    * clean up any tmp files
    */
   public void cleanup()
   {
       log.info("cleaning up");
      if (this.docleanup)
      {
         if (tmpDirFile != null)
         {
            MetaUtils.recursiveDelete(tmpDirFile);
            tmpDirFile = null;
         }
      }
       log.info("finished cleaning up");
   }

  

   /**
    * given a list of sequences, creates a db for use with blast using
    * formatdb executable
    *
    * @param seqList is the list of sequences to create the database with
    * @return the full path of the location of the database
    * @throws IOException if it can't read or write to the files
    * @throws InterruptedException if system.exec is interrupted.
    */
   private String execFormatDB(Map<String, String> seqList) throws IOException, InterruptedException
   {
       log.info("running formatdb");
      BufferedWriter out;

      log.debug("blastcmd: formating the sequence db using formatdb");

      /*
       * open temp file
       */
      log.debug("blastcmd: using temp directory " + tmpDirFile.getPath());
      out = new BufferedWriter(new FileWriter(tmpDirFile.getPath() + "/seqfile"));

      /*
       *  write out the sequences to file
       */
      for (String key : seqList.keySet())
      {
         assert(seqList.get(key) != null);
         out.write(">" + key + "\n");
         out.write(seqList.get(key) + "\n");
      }

      /*
       *  close temp file
       */
      out.close();

      databasesize = new File(tmpDirFile.getPath() + "/seqfile").length();

      log.debug("blastcmd: done writing sequence file");

      /*
       * execute formatdb command
       */

      List<String> commands = new ArrayList<String>();
      File         seqFile  = new File(tmpDirFile, "seqfile");
      commands.add("/bin/sh");
      commands.add("-c");
      commands.add("export TMPDIR=" + tmpDirFile.getPath() + "; cd " + tmpDirFile.getPath() + ";" + formatdbCommandPath + " " + formatdbCommandLine + " -i " + seqFile.getPath() + " -n " + "seqfile");

      log.info("blastcmd: formatdbcommand = " + commands);

      SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
      int r = commandExecutor.executeCommand();

      log.debug("return value = " + r);
      log.debug("stdout = " + commandExecutor.getStandardOutputFromCommand().toString());
      log.debug("stderr = " + commandExecutor.getStandardErrorFromCommand().toString());

       log.info("done with formatdb, exit value = " + r);

      return(seqFile.getPath());
   }


   /**
    * copies a file from DFS to local working directory
    *
    * @param dfsPath is the pathname to a file in DFS
    * @return the path of the new file in local scratch space
    * @throws IOException if it can't access the files
    */
   private String copyDBFile(String dfsPath) throws IOException
   {
       log.info("copiing database to file");

      Configuration conf = new Configuration();
      FileSystem    fs   = FileSystem.get(conf);

      Path filenamePath = new Path(dfsPath);
      File localFile    = new File(tmpDirFile, filenamePath.getName());

      if (!fs.exists(filenamePath))
      {
         throw new IOException("file not found: " + dfsPath);
      }

      FSDataInputStream in = fs.open(filenamePath);
      BufferedReader    d
         = new BufferedReader(new InputStreamReader(in));

      BufferedWriter out = new BufferedWriter(new FileWriter(localFile.getPath()));

      String line;
      line = d.readLine();

      while (line != null)
      {
         out.write(line + "\n");
         line = d.readLine();
      }
      in.close();
      out.close();

       log.info("done with data copy");
      return(localFile.getPath());
   }


   /**
    * execute the blast command and return a list of sequence ids that match
    *
    * @param seqMap is the key/value map of sequences that act as reference keyed by name
    * @param cazyEC  is the full path of the cazy database to search against the reference
    * @return a list of sequence ids in the reference that match the cazy database
    * @throws IOException if it can't access files for whatever reason,
    * @throws InterruptedException if system.exec is interrupted
    */
   public Set<String> exec(Map<String, String> seqMap, String cazyEC) throws IOException, InterruptedException
   {
       log.info("starting exec");
      String seqDir      = execFormatDB(seqMap);
      String localCazyEC = copyDBFile(cazyEC);

      if (seqDir == null)
      {
         /*
          * didn't run formatdb, so return with fail
          */
         throw new IOException("blast can't run... formatdb did not process the inputs");
      }

      List<String> commands = new ArrayList<String>();
      commands.add("/bin/sh");
      commands.add("-c");

      commandString = new StringBuffer();

       commandString.append("cd " + tmpDirFile.getPath() + "; " + commandPath + " " + commandLine);
       if (this.useEffectiveSize) {
           commandString.append(" -z " + effectiveSize);
       }
       double ecutoff = this.useEValue;
       if (this.useScaledEValue) {
           ecutoff = (ecutoff * databasesize / effectiveSize);
       }
       commandString.append(" -e " + ecutoff);
       commandString.append(" -d " + seqDir + " -i " + localCazyEC);

       log.info("command = " + commandString.toString());

      commands.add(commandString.toString());

      // TODO: remove the try statement to throw exception in case of failure

         SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
         exitValue = commandExecutor.executeCommand();

         // stdout and stderr of the command are returned as StringBuilder objects
         stdout = commandExecutor.getStandardOutputFromCommand().toString();
         stderr = commandExecutor.getStandardErrorFromCommand().toString();

         log.debug("exit = " + exitValue);
         log.debug("stdout = " + stdout);
         log.debug("stderr = " + stderr);

      if (exitValue != 0) {
          log.error("blast executable exit value = " + exitValue + ": " + stderr);
          throw new IOException("blast executable exit value = " + exitValue + ": " + stderr);
      }

      /*
       * now parse the output
       */
      String[] lines = stdout.split("\n");
      Set<String> s = new HashSet<String>();

      s.addAll(Arrays.asList(lines));

       log.info("done with exec, exit value = " + exitValue);
      return(s);
   }

}
