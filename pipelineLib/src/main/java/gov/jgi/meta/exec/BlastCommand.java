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
    String DEFAULTTMPDIR = "/tmp/blast";

    // blastall -m 8 -p tblastn -b 1000000 -a 10 -o $workdir/cazy.blastout -d $blast_db -i $cazy

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
    Logger log = Logger.getLogger(BlastCommand.class);

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
     * flag to leave working directories along (if false) or to remove
     * them after execution (if true)
     */
    Boolean cleanup = true;

    /**
     * new blast command based on default parameters
     */
    public BlastCommand() throws IOException {
        // look in configuration file to determine default values
        commandLine = DEFAULTCOMMANDLINE;
        commandPath = DEFAULTCOMMANDPATH;
        tmpDir = DEFAULTTMPDIR;
        tmpDirFile = createTempDir();
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
    public BlastCommand(Configuration config) throws IOException {

        String c;

        log.info("initializing new blast command");

        if ((c = config.get("blast.commandline")) != null) {
            commandLine = c;
        } else {
            commandLine = DEFAULTCOMMANDLINE;
        }
        if ((c = config.get("blast.commandpath")) != null) {
            commandPath = c;
        } else {
            commandPath = DEFAULTCOMMANDPATH;
        }
        if ((c = config.get("blast.tmpdir")) != null) {
            tmpDir = c;
        } else {
            tmpDir = DEFAULTTMPDIR;
        }

        cleanup = config.getBoolean("blast.cleanup", true);

        /*
        do sanity check to make sure all paths exist
         */
        checkFileExists(commandLine);
        checkFileExists(commandPath);
        checkDirExists(tmpDir);

        /*
        if all is good, create a working space inside tmpDir
         */

        tmpDirFile = createTempDir();

    }

    private int checkFileExists(String filePath) throws IOException {
        // TODO: fill out this function
        return 0;
    }

    private int checkDirExists(String filePath) throws IOException {
        // TODO: fill out this function
        return 0;
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
        if (tmpDirFile != null) {
            recursiveDelete(tmpDirFile);
            tmpDirFile = null;
        }

        super.finalize();
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
    private String execFormatDB(Map<String, String> seqList) throws IOException, InterruptedException {

        File tmpdir;
        BufferedWriter out;

        log.debug("blastcmd: formating the sequence db using formatdb");

        /*
        open temp file
         */
        log.debug("blastcmd: using temp directory " + tmpDirFile.getPath());

        out = new BufferedWriter(new FileWriter(tmpDirFile.getPath() + "/seqfile"));

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

        log.debug("blastcmd: done writing sequence file");

        /*
        execute formatdb command
         */

        List<String> commands = new ArrayList<String>();
        File seqFile = new File(tmpDirFile, "seqfile");
        commands.add("/bin/sh");
        commands.add("-c");
        commands.add("cd " + tmpDirFile.getPath() + ";" + " formatdb -o T -p F -i " + seqFile.getPath() + " -n " + "seqfile");

        log.debug("blastcmd: formatdbcommand = " + commands);

        SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
        int r = commandExecutor.executeCommand();

        log.debug("return value = " + r);
        log.debug("stdout = " + commandExecutor.getStandardOutputFromCommand().toString());
        log.debug("stderr = " + commandExecutor.getStandardErrorFromCommand().toString());

        return seqFile.getPath();
    }

    /**
     * copies a file from DFS to local working directory
     *
     * @param dfsPath is the pathname to a file in DFS
     * @return the path of the new file in local scratch space
     * @throws IOException if it can't access the files
     */
    private String copyDBFile(String dfsPath) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filenamePath = new Path(dfsPath);
        File localFile = new File(tmpDirFile, filenamePath.getName());

        if (!fs.exists(filenamePath)) {
            throw new IOException("file not found: " + dfsPath);
        }

        FSDataInputStream in = fs.open(filenamePath);
        BufferedReader d
                  = new BufferedReader(new InputStreamReader(in));

        BufferedWriter out = new BufferedWriter(new FileWriter(localFile.getPath()));

        String line;
        line = d.readLine();

        while (line != null) {
            out.write(line+"\n");
            line = d.readLine();
        }
        in.close();
        out.close();

        return localFile.getPath();
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
    public Set<String> exec(Map<String, String> seqMap, String cazyEC) throws IOException, InterruptedException {

        String seqDir = execFormatDB(seqMap);
        String localCazyEC = copyDBFile(cazyEC);

        if (seqDir == null) {
            /*
             didn't run formatdb, so return with fail
            */
            return null;

        }

        List<String> commands = new ArrayList<String>();
        commands.add("/bin/sh");
        commands.add("-c");
        commands.add(commandPath + " " + commandLine + " -d " + seqDir + " -i " + localCazyEC);


        // TODO: remove the try statement to throw exception in case of failure

        try {

            log.debug("command = " + commands);
            SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
            exitValue = commandExecutor.executeCommand();

            // stdout and stderr of the command are returned as StringBuilder objects
            stdout = commandExecutor.getStandardOutputFromCommand().toString();
            stderr = commandExecutor.getStandardErrorFromCommand().toString();

            log.debug("exit = " + exitValue);
            log.debug("stdout = " + stdout);
            log.debug("stderr = " + stderr);

        } catch (Exception e) {
            log.error(e);
            return null;
        }

        /*
        now parse the output
         */
        String[] lines = stdout.split("\n");
        Set<String> s = new HashSet<String>();

        s.addAll(Arrays.asList(lines));

        return s;
    }


    /**
     * Create a new temporary directory. Use something like
     * {@link #recursiveDelete(File)} to clean this directory up since it isn't
     * deleted automatically
     *
     * @return the new directory
     * @throws IOException if there is an error creating the temporary directory
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

        conf.addResource("blast-test-conf.xml");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        /*
        process arguments
         */

        if (otherArgs.length != 2) {
            System.err.println("Usage: blast <seqfilepath> <ecfilepath>");
            System.exit(2);
        }


        Map<String,String> l = new HashMap<String,String>();
        Set<String> r;

        Text t = new Text();
        FileInputStream fstream = new FileInputStream(otherArgs[0]);
        FastaBlockLineReader in = new FastaBlockLineReader(fstream);
        int bytes = in.readLine(t, l);

        BlastCommand b = new BlastCommand();
        r = b.exec(l, otherArgs[1]);

        System.out.println("matches = " + r);
    }
}
