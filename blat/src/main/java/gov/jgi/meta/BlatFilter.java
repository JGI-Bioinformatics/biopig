/*
 * Copyright (c) 2010, Joint Genome Institute (JGI) United States Department of Energy
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    This product includes software developed by the JGI.
 * 4. Neither the name of the JGI nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
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


package gov.jgi.meta;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import gov.jgi.meta.hadoop.input.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.log4j.Logger;


/**
 * custom counters
 */
enum BlatCounters {
    /**
     * number of blat commands that return sucessfully
     */
    NUMBER_OF_SUCCESSFUL_BLATCOMMANDS,
    /**
     * number of blat commands that return with error
     */
    NUMBER_OF_ERROR_BLATCOMMANDS,
    /**
     * total number of reads in the database.
     */
    NUMBER_OF_READS,
    /**
     * total number of reads that matched in all groups
     * (duplicates possible, eg, read x matches to multiple
     * groups).
     */
    NUMBER_OF_MATCHED_READS,
    /**
     * total number of gene groups after blat expansion
     * (should be same as number of groups that were read in)
     */
    NUMBER_OF_GROUPS,
    /**
     * total number of map tasks
     */
    NUMBER_OF_MAP_TASKS,
    /**
     * total number of reduce tasks
     */
    NUMBER_OF_REDUCE_TASKS

}


/**
 * hadoop application to read sequence reads from file perform BLAT
 * operations against constant database.
 * <p/>
 * Set the following properties in a file called: blat-conf.xml that should
 * be in the classpath of hadoop.
 * <p/>
 */
public class BlatFilter {

    /**
     * starts off the hadoop application
     *
     * @param args specify input file cassandra host and kmer size
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Logger log = Logger.getLogger(BlatFilter.class);



        /*
         see the application configuration file for details on what options are read in.
         */
        Configuration conf = new Configuration();
        conf.addResource("blat-conf.xml");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();


        /*
        process arguments
         */

        if (otherArgs.length != 3) {
            System.err.println("Usage: blat <readfile> <blatoutputfile> <outputdir>");
            System.exit(2);
        }

        conf.set("blastoutputfile", otherArgs[1]);

        /*
        this improves performance of the file i/o
         */
        conf.setInt("io.file.buffer.size", 1024 * 1024);

        log.info("main() [version " + conf.get("version", "unknown!")+ "] starting with following parameters");
        log.info("\tsequence file: " + otherArgs[0]);
        log.info("\tgene db file : " + otherArgs[1]);
        log.info("\t-------------------------------");
        log.info("\tOptions:");

        String[] optionalProperties = {
                "mapred.min.split.size",
                "mapred.max.split.size",
                "blat.commandline",
                "blat.commandpath",
                "blat.tmpdir",
                "blat.cleanup",
                "blat.skipexecution",
                "blat.paired",
                "blat.numreducers"
        };
        for (String option : optionalProperties) {
            if (conf.get(option) != null) {
                log.info("\toption " + option + ":\t" + conf.get(option));
            }
        }

        /*
        setup blast configuration parameters
         */

        Job job = new Job(conf, "loader");
        job.setJarByClass(BlatFilter.class);
        job.setInputFormatClass(FastaBlockInputFormat.class);
        job.setMapperClass(FastaTokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(conf.getInt("blat.numreducers", 1));

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}



