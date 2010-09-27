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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;


/**                   
 * class that wraps execution of commandline velvet program.
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
    String DEFAULTCOMMANDPATH = "/home/asczyrba/bin/cap3";
    String DEFAULTTMPDIR = "/tmp/cap3";

    /**
     * logger
     */
    Logger log = Logger.getLogger(CapCommand.class);

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
    String tmpDir = null;
    File tmpDirFile = null;

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
     */
    public CapCommand() throws IOException {
        // look in configuration file to determine default values
        commandLine = DEFAULTCOMMANDLINE;
        commandPath = DEFAULTCOMMANDPATH;
        tmpDir = DEFAULTTMPDIR;

        
        tmpDirFile = MetaUtils.createTempDir(tmpDir);
    }


    public void cleanup(){

       if (this.doCleanup) {
            if (tmpDirFile != null) {
                MetaUtils.recursiveDelete(tmpDirFile);
                tmpDirFile = null;
            }
        }

    }
    /**
     * new blast command based on values stored in the configuration.
     * <p/>
     * Looks for the following config values: blast.commandline,
     * blast.commandpath, and blast.tmpdir
     *
     * @param config is the hadoop configuration with overriding values
     *               for commandline options and paths
     */
    public CapCommand(Configuration config) throws IOException {

        String c;

        if ((c = config.get("cap3.commandline")) != null) {
            commandLine = c;
        } else {
            commandLine = DEFAULTCOMMANDLINE;
        }
        if ((c = config.get("cap3.commandpath")) != null) {
            commandPath = c;
        } else {
            commandPath = DEFAULTCOMMANDPATH;
        }
        if ((c = config.get("assembler.tmpdir")) != null) {
            tmpDir = c;
        } else {
            tmpDir = DEFAULTTMPDIR;
        }

        doCleanup = config.getBoolean("assembler.cleanup", true);

         /*
         do sanity check to make sure all paths exist
          */
         //checkFileExists(commandLine);
         //checkFileExists(commandPath);
         //checkDirExists(tmpDir);

        /*
        if all is good, create a working space inside tmpDir
         */

        tmpDirFile = MetaUtils.createTempDir(tmpDir);

    }

    /**
     * destructor deletes the tmp space if it was created
     *
     * @throws Throwable
     */
    protected void finalize() throws Throwable {
        /*
        delete the tmp files if they exist
         */
        log.info("deleting tmp file: " + tmpDirFile.getPath());

        cleanup();

        super.finalize();
    }




    /**
     * execute the blast command and return a list of sequence ids that match
     *
     * @param seqDatabase is the key/value map of sequences that act as reference keyed by name
     * @return a list of sequence ids in the reference that match the cazy database
     */
    public Map<String,String> exec(String groupId, Map<String, String> seqDatabase, Reducer.Context context)  throws IOException, InterruptedException  {

        Map<String, String> s = new HashMap<String, String>();

        /*
        first, take the blatInputFile and find the corresponding sequence in the
        seqMap.  find both the exact sequence id, as well as its matching pair
        and write to temporary file.
        */
        File seqQueryFile = null;
        Long executionStartTime = new Date().getTime();

        log.info("Preparing Assembly execution");
        if (context != null) context.setStatus("Preparing Assembly execution");

        int numGroups = 0;
        int numReads = 0;

        /*
        dump the database from the map to a file
         */
        String seqFilepath = MetaUtils.sequenceToLocalFile(seqDatabase, tmpDirFile+"/reads.fa");

        if (seqFilepath == null) {
            /*
            return with fail
             */
            return null;
        }

        if (context != null) context.setStatus("running cap3 with " + seqDatabase.size() + " reads");
            /*
            now set up a blat execution
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
        if (context != null) context.getCounter("reduce.assembly", "EXECUTION_TIME").increment(executionTime);
        
        /*
            now parse the output and clean up
            */

        try {

            Text t = new Text();
            FileInputStream fstream = new FileInputStream(tmpDirFile.getPath()+"/reads.fa.cap.contigs");
            FastaBlockLineReader in = new FastaBlockLineReader(fstream);
            int bytes = in.readLine(t, s);

        } catch (Exception e) {
            log.error("unable to find outputfile:" + e);
            return null;
        }

        return s;
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.addResource("assembly-test-conf.xml");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        CapCommand capCmd = new CapCommand(conf);

        /*
        process arguments
         */

        if (otherArgs.length != 1) {
            System.err.println("Usage: assembly <blatoutput>");
            System.exit(2);
        }

        FileReader input = new FileReader(otherArgs[0]);

        /*
                Filter FileReader through a Buffered read to read a line at a time
                */
        BufferedReader bufRead2 = new BufferedReader(input);

        String line2;    // String that holds current file line
        int count = 0;  // Line number of count

        // Read first line
        line2 = bufRead2.readLine();

        // Read through file one line at time. Print line # and line
        while (line2 != null){
            String[] a = line2.split("\t",2);
            String groupId = a[0];

            Map<String,String> s = null;
            Map<String,String> map = new HashMap<String,String>();

            for (String read : a[1].split("\t")) {
                String[] b = read.split("&",2);
                map.put(b[0],b[1]);
            }

            s = capCmd.exec(groupId, map, null);

            for (String contigid : s.keySet()) {
                System.out.println(contigid + ": " + s.get(contigid));
            }

            line2 = bufRead2.readLine();
            count++;
        }

        bufRead2.close();

    }
}