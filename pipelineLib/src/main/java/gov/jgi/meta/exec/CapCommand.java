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

package gov.jgi.meta.exec;

import com.devdaily.system.SystemCommandExecutor;
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

        tmpDirFile = createTempDir();
    }


    public void cleanup(){

       if (this.doCleanup) {
            if (tmpDirFile != null) {
                recursiveDelete(tmpDirFile);
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

        tmpDirFile = createTempDir();

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
     * given a list of sequences, creates a db for use with cap3
     *
     * @param seqList is the list of sequences to create the database with
     * @return the full path of the location of the database
     */
    private String dumpToFile(Map<String, String> seqList) {

        BufferedWriter out;
        File seqFile = null;

        /*
        open temp file
         */
        try {
            seqFile = new File(tmpDirFile, "reads.fa");
            out = new BufferedWriter(new FileWriter(seqFile.getPath()));

            /*
            write out the sequences to file
            */
            for (String key : seqList.keySet()) {
                assert(seqList.get(key) != null);
                out.write(">" + key + "\n");
                out.write(seqList.get(key) + "\n");
            }

            /*
            close temp file
             */
            out.close();

        } catch (Exception e) {
            log.error(e);
            return null;
        }


        return seqFile.getPath();
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

        log.info("Preparing Assembly execution");
        if (context != null) context.setStatus("Preparing Assembly execution");

        int numGroups = 0;
        int numReads = 0;

        /*
        dump the database from the map to a file
         */
        String seqFilepath = dumpToFile(seqDatabase);

        if (seqFilepath == null) {
            /*
            return with fail
             */
            return null;
        }

        if (context != null) context.setStatus("Executing Assembler");
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


    /**
     * Create a new temporary directory. Use something like
     * {@link #recursiveDelete(java.io.File)} to clean this directory up since it isn't
     * deleted automatically
     *
     * @return the new directory
     * @throws java.io.IOException if there is an error creating the temporary directory
     */
    public File createTempDir() throws IOException {
        final File sysTempDir = new File(tmpDir);
        File newTempDir;
        final int maxAttempts = 9;
        int attemptCount = 0;
        do {
            attemptCount++;
            if (attemptCount > maxAttempts) {
                throw new IOException(
                        "The highly improbable has occurred! Failed to " +
                                "create a unique temporary directory after " +
                                maxAttempts + " attempts.");
            }
            String dirName = UUID.randomUUID().toString();
            newTempDir = new File(sysTempDir, dirName);
        } while (newTempDir.exists());

        if (newTempDir.mkdirs()) {
            return newTempDir;
        } else {
            throw new IOException(
                    "Failed to create temp dir named " +
                            newTempDir.getAbsolutePath());
        }
    }

    /**
     * Recursively delete file or directory
     *
     * @param fileOrDir the file or dir to delete
     * @return true iff all files are successfully deleted
     */
    public boolean recursiveDelete(File fileOrDir) {
        if (fileOrDir.isDirectory()) {
            // recursively delete contents
            for (File innerFile : fileOrDir.listFiles()) {
                if (!recursiveDelete(innerFile)) {
                    return false;
                }
            }
        }

        return fileOrDir.delete();
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