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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;


/**
 * class that wraps execution of commandline BLAT program.
 * <p/>
 * Use this by creating a new BlatCommand than invoking the
 * exec method.  eg:
 * <p/>
 * BlatCommand b = new BlatCommand();
 * r = b.exec(l, otherArgs[1]);
 * <p/>
 * Exec dumps input seqence database (l) into a file, then
 * reads the groups from the second argument.  For each group,
 * create a temp file holding the sequences of interest (determined
 * by looking them up in the database), and then executing blat
 * as system process.
 */

public class BlatCommand {


    String DEFAULTCOMMANDLINE = "-out=blast8";
    String DEFAULTCOMMANDPATH = "/jgi/tools/bin/blat";
    String DEFAULTTMPDIR = "/tmp/blat";

    /**
     * logger
     */
    Logger log = Logger.getLogger(BlatCommand.class);

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
    Boolean cleanup = true;

    Boolean paired = true;

    /**
     * new blast command based on default parameters
     */
    public BlatCommand() throws IOException {
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
     * blast.commandpath, and blast.tmpdir
     *
     * @param config is the hadoop configuration with overriding values
     *               for commandline options and paths
     */
    public BlatCommand(Configuration config) throws IOException {

        String c;

        if ((c = config.get("blat.commandline")) != null) {
            commandLine = c;
        } else {
            commandLine = DEFAULTCOMMANDLINE;
        }
        if ((c = config.get("blat.commandpath")) != null) {
            commandPath = c;
        } else {
            commandPath = DEFAULTCOMMANDPATH;
        }
        if ((c = config.get("blat.tmpdir")) != null) {
            tmpDir = c;
        } else {
            tmpDir = DEFAULTTMPDIR;
        }

        cleanup = config.getBoolean("blat.cleanup", true);
        paired = config.getBoolean("blat.paired", true);

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

        if (tmpDirFile != null) {
            if (cleanup) recursiveDelete(tmpDirFile);
            tmpDirFile = null;
        }

        super.finalize();
    }


    /**
     * given a list of sequences, creates a db for use with blat
     *
     * @param seqList is the list of sequences to create the database with
     * @return the full path of the location of the database
     */
    private String dumpToFile(Map<String, String> seqList) {

        File tmpdir;
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
                assert (seqList.get(key) != null);
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
            out.write(line + "\n");
            line = d.readLine();
        }
        in.close();
        out.close();

        return localFile.getPath();
    }


    /**
     * execute the blast command and return a list of sequence ids that match
     *
     * @param seqDatabase      is the key/value map of sequences that act as reference keyed by name
     * @param seqQueryFilepath is the full path of the cazy database to search against the reference
     * @return a list of sequence ids in the reference that match the cazy database
     */
    public Set<String> exec(Map<String, String> seqDatabase, String seqQueryFilepath, Mapper.Context context) throws IOException, InterruptedException {

        /*
        first, take the blatInputFile and find the corresponding sequence in the
        seqMap.  find both the exact sequence id, as well as its matching pair
        and write to temporary file.
        */
        //Map<String,String> l = new HashMap<String,String>();

        File seqQueryFile = null;
        File tmpdir;

        log.info("Preparing Blat execution");
        context.setStatus("Preparing Blat execution");

        Map<String, String> l = new HashMap<String, String>();
        int numGroups = 0;
        int numReads = 0;

        /*
       open query file.
        */

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filenamePath = new Path(seqQueryFilepath);

        if (!fs.exists(filenamePath)) {
            throw new IOException("file not found: " + seqQueryFilepath);
        }

        FSDataInputStream in = fs.open(filenamePath);
        BufferedReader bufRead
                = new BufferedReader(new InputStreamReader(in));

        /*
        Filter FileReader through a Buffered read to read a line at a time
        */

        String line = bufRead.readLine();    // String that holds current file line

        /*
       read the line into key/value with key being the first column, value is all the
       remaining columns
        */
        while (line != null) {
            numGroups++;
            String[] a = line.split("\t", 2);
            l.put(a[0], a[1]);
            numReads += a[1].split("\t").length;
            line = bufRead.readLine();
        }
        bufRead.close();


        log.info("read " + numReads + " Reads in " + numGroups + " gene groups");

        /*
        now dump the database from the map to a file
         */
        String seqFilepath = dumpToFile(seqDatabase);

        if (seqFilepath == null) {
            /*
            return with fail
             */
            return null;
        }

        Map<String, Set<String>> s = new HashMap<String, Set<String>>();

        /*
        now loop through all the lines previously read in, write out a seqfile in temp directory
        then execute blat.
         */
        int numBlats = 0;
        int totalBlats = l.size();

        for (String k : l.keySet()) {
            numBlats++;

            /*
            k is a grouping key
             */

            log.info("processing group " + k);

            context.setStatus("Executing Blat " + numBlats + "/" + totalBlats);
            /*
           create a new file in temp direectory
            */
            seqQueryFile = new File(tmpDirFile, "blatquery.fa");
            BufferedWriter out = new BufferedWriter(new FileWriter(seqQueryFile.getPath()));

            /*
           look up all the sequences and write them to the file.  include the paired ends
            */
            int queryCount = 0;
            for (String key : l.get(k).split("\t")) {

                if (paired) {

                    /*
                    for paired end data, look for both pairs
                     */

                    String key1 = key + "/1";  // forward
                    String key2 = key + "/2";  // backward

                    if (seqDatabase.containsKey(key1)) {
                        queryCount++;
                        out.write(">" + key1 + "\n");
                        out.write(seqDatabase.get(key1) + "\n");
                    }
                    if (seqDatabase.containsKey(key2)) {
                        queryCount++;
                        out.write(">" + key2 + "\n");
                        out.write(seqDatabase.get(key2) + "\n");
                    }
                } else {

                    /*
                    if data is not paired, just look up key
                     */

                    if (seqDatabase.containsKey(key)) {
                        queryCount++;
                        out.write(">" + key + "\n");
                        out.write(seqDatabase.get(key) + "\n");
                    }
                }
            }
            /*
            close the temporary file
            */
            out.close();

            if (queryCount == 0) {
                /*
               means that none of these queries were in this portion of the database.  no point
               executing blat, so just return
                */
                log.info("skipping blat since i didn't find any query sequences in this database");
                continue;
            }

            /*
            now set up a blat execution
             */
            List<String> commands = new ArrayList<String>();
            commands.add("/bin/sh");
            commands.add("-c");
            commands.add(commandPath + " " + commandLine + " " + seqFilepath + " " + seqQueryFile.getPath() + " " +
                    tmpDirFile.getPath() + "/blat.output");


            log.info("command = " + commands);

            SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
            exitValue = commandExecutor.executeCommand();

            // stdout and stderr of the command are returned as StringBuilder objects
            stdout = commandExecutor.getStandardOutputFromCommand().toString();
            stderr = commandExecutor.getStandardErrorFromCommand().toString();

            log.info("exit = " + exitValue);
            log.info("stdout = " + stdout);
            log.info("stderr = " + stderr);


            /*
            now parse the output and clean up
            */

            try {

                log.info("reading outputfile: " + tmpDirFile.getPath() + "/blat.output");

                FileReader input = new FileReader(tmpDirFile.getPath() + "/blat.output");

                /*
                Filter FileReader through a Buffered read to read a line at a time
                */
                BufferedReader bufRead2 = new BufferedReader(input);

                String line2;    // String that holds current file line
                int count = 0;  // Line number of count

                // Read first line
                line2 = bufRead2.readLine();

                // Read through file one line at time. Print line # and line
                while (line2 != null) {
                    String[] a = line2.split("\t", 3);
                    if (s.containsKey(k)) {
                        s.get(k).add(a[1]);
                    } else {
                        s.put(k, new HashSet<String>());
                        s.get(k).add(a[1]);                        
                    }
                    line2 = bufRead2.readLine();
                    count++;
                }

                bufRead2.close();

                log.info("done reading file");
            } catch (Exception e) {
                log.error(e);
            }

            /*
            should clean up - note: files get overwritten, so don't worry about it. :-)
             */

        }

        context.setStatus("Postprocessing Blat output");
        /*
        post processing.  since i need to return in the format of
        <groupid> <readid1> <readid2> <readid3> ...
        as a single string (one string per line).
         */

        log.info("Postprocessing Blat");
        log.info("  numGroups = " + s.keySet().size());

        Set<String> ss = new HashSet<String>();

        for (String k : s.keySet()) {
            StringBuilder stringBuilder = new StringBuilder();
            for (Iterator iter = s.get(k).iterator(); iter.hasNext();) {
                stringBuilder.append(", " + iter.next());
            }
            ss.add(k + stringBuilder);
        }

        return ss;
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

        conf.addResource("blat-test-conf.xml");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        /*
        process arguments
         */

        if (otherArgs.length != 2) {
            System.err.println("Usage: blat <seqfilepath> <ecfilepath>");
            System.exit(2);
        }

        Map<String, String> l = new HashMap<String, String>();
        Set<String> r;

        Text t = new Text();
        FileInputStream fstream = new FileInputStream(otherArgs[0]);
        FastaBlockLineReader in = new FastaBlockLineReader(fstream);
        int bytes = in.readLine(t, l);

        BlatCommand b = new BlatCommand();
        r = b.exec(l, otherArgs[1], null);

        System.out.println("matches = " + r);
    }
}