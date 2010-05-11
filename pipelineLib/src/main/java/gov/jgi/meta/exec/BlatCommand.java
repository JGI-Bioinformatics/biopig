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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;


/**
 * class that wraps execution of commandline BLAT program.
 */

public class BlatCommand {


    String DEFAULTCOMMANDLINE = "-out=blast8";
    String DEFAULTCOMMANDPATH = "blat";
    String DEFAULTTMPDIR = "/tmp/blat";

    // get commandline from configuration parameters

    // blastall -m 8 -p tblastn -b 1000000 -a 10 -o $workdir/cazy.blastout -d $blast_db -i $cazy

    // -p program name
    // -m 8 alignment view options - tabular
    // -b 1000000 number of sequences to show alignments of (max number)
    // -a 10 number of processors to use

    // -o output file
    // -d blast database directory where formatdb was run
    // -i input sequence

    // need to return list of id's of hits

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
     * new blast command based on default parameters
     */
    public BlatCommand() {
        // look in configuration file to determine default values
        commandLine = DEFAULTCOMMANDLINE;
        commandPath = DEFAULTCOMMANDPATH;
        tmpDir = DEFAULTTMPDIR;
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
    public BlatCommand(Configuration config) {

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
            tmpdir = createTempDir();
            seqFile = new File(tmpdir, "reads.fa");
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
     * @param seqQueryFilepath  is the full path of the cazy database to search against the reference
     * @return a list of sequence ids in the reference that match the cazy database
     */
    public Set<String> exec(Map<String, String> seqDatabase, String seqQueryFilepath) {

        /*
        first, take the blatInputFile and find the corresponding sequence in the
        seqMap.  find both the exact sequence id, as well as its matching pair
        and write to temporary file.
        */
        //Map<String,String> l = new HashMap<String,String>();

        File seqQueryFile = null;
        File tmpdir;

        log.info("Preparing Blat execution");

        Map<String, String> l = new HashMap<String, String>();
        int numGroups = 0;
        int numReads = 0;
        try {
             FileReader input = new FileReader(seqQueryFilepath);

            /* Filter FileReader through a Buffered read to read a line at a
               time */
             BufferedReader bufRead = new BufferedReader(input);

             String line;    // String that holds current file line

            // Read first line
            line = bufRead.readLine();

            // Read through file one line at time. Print line # and line
            while (line != null){
                numGroups++;
                String[] a = line.split("\t", 2);
                l.put(a[0], a[1]);
                numReads += a[1].split("\t").length;
                line = bufRead.readLine();
            }
            bufRead.close();

         } catch (Exception e) {
             log.error(e);
             return null;
         }

         log.info("read " + numReads + " Reads in " + numGroups + " gene groups");

//        try {
//            Text t = new Text();
//            FileInputStream fstream = new FileInputStream(seqQueryFilepath);
//            FastaBlockLineReader in = new FastaBlockLineReader(fstream);
//            int bytes = in.readLine(t, l);
//        } catch (Exception e) {
//            log.error(e);
//            return null;
//        }

        String seqFilepath = dumpToFile(seqDatabase);

        if (seqFilepath == null) {
            /*
            didn't run formatdb, so return with fail
             */
            return null;
        }

        try {
            tmpdir = createTempDir();
        } catch (Exception e) {
            log.error(e);
            return null;
        }

        Map<String, String> s = new HashMap<String, String>();

        for (String k : l.keySet()) {
            try {
                seqQueryFile = new File(tmpdir, "blatquery.fa");
                BufferedWriter out = new BufferedWriter(new FileWriter(seqQueryFile.getPath()));
                for (String key : l.get( k ).split("\t")) {
                    String key1 = key+"/1";
                    String key2 = key+"/2";
                    if (seqDatabase.containsKey(key1)) {
                        out.write(">" + key1 + "\n");
                        out.write(seqDatabase.get(key1) + "\n");
                    }
                    if (seqDatabase.containsKey(key2)) {
                        out.write(">" + key2 + "\n");
                        out.write(seqDatabase.get(key2) + "\n");
                    }
                }
                out.close();
            } catch (Exception e) {
                log.error(e);
                return null;
            }

            List<String> commands = new ArrayList<String>();
            commands.add("/bin/sh");
            commands.add("-c");
            commands.add(commandPath + " " + commandLine + " " + seqFilepath + " " + seqQueryFile.getPath() + " blat.output");

            try {

                log.debug("command = " + commands);
                SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
                exitValue = commandExecutor.executeCommand();


                // stdout and stderr of the command are returned as StringBuilder objects
                stdout = commandExecutor.getStandardOutputFromCommand().toString();
                stderr = commandExecutor.getStandardErrorFromCommand().toString();

                //log.debug("exit = " + exitValue);
                //log.debug("stdout = " + stdout);
                //log.debug("stderr = " + stderr);

            } catch (Exception e) {
                log.error(e);
                return null;
            }

        /*
        now parse the output
         */


            try {
                FileReader input = new FileReader("blat.output");

                /* Filter FileReader through a Buffered read to read a line at a
              time */
                BufferedReader bufRead = new BufferedReader(input);

                String line;    // String that holds current file line
                int count = 0;  // Line number of count

                // Read first line
                line = bufRead.readLine();

                // Read through file one line at time. Print line # and line
                while (line != null){
                    String[] a = line.split("\t");
                    if (s.containsKey(k)) {
                        s.put(k, s.get(k) + "\t" + a[1]);
                    } else {
                        s.put(k, a[1]);
                    }
                    line = bufRead.readLine();
                    count++;
                }

                bufRead.close();

            } catch (Exception e) {
                log.error(e);
                return null;
            }
        }

        log.info("Postprocessing Blat");
        log.info("  numGroups = " + s.keySet().size());
        int xcount = 0;
        Set<String> ss = new HashSet<String>();
        for (String x : s.keySet()) {
            int xx = s.get(x).split("\t").length;
            xcount += xx;
            ss.add(x + "\t" + s.get(x));
        }
        log.info("  numReads = " + xcount);
        log.info("  avgSize/group = " + xcount/s.keySet().size());
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

        Map<String,String> l = new HashMap<String,String>();
        Set<String> r;

        Text t = new Text();
        FileInputStream fstream = new FileInputStream(otherArgs[0]);
        FastaBlockLineReader in = new FastaBlockLineReader(fstream);
        int bytes = in.readLine(t, l);

        BlatCommand b = new BlatCommand();
        r = b.exec(l, otherArgs[1]);

        System.out.println("matches = " + r);
    }
}