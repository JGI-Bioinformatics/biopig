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



package gov.jgi.meta;

import java.util.*;

import gov.jgi.meta.hadoop.input.*;
import gov.jgi.meta.hadoop.map.BlastMapperGroupByGene;
import gov.jgi.meta.hadoop.map.BlatMapperByGroup;
import gov.jgi.meta.hadoop.reduce.AssembleByGroupKey;
import gov.jgi.meta.hadoop.reduce.IdentityReducerGroupByKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.log4j.Logger;


/**
 * custom counters
 */
enum AppCounters {

    NUMBER_OF_SUCCESSFUL_BLASTCOMMANDS,
    NUMBER_OF_SUCCESSFUL_BLATCOMMANDS,
    NUMBER_OF_SUCCESSFUL_ASSEMBLYCOMMANDS,

    NUMBER_OF_ERROR_BLASTCOMMANDS,
    NUMBER_OF_ERROR_BLATCOMMANDS,
    NUMBER_OF_ERROR_ASSEMBLYCOMMANDS,

    /** the total number of matched reads (not unique, could be duplicates)
     */
    NUMBER_OF_MATCHED_READS,
    NUMBER_OF_MATCHED_READS_AFTER_BLAST,

    /** the total number of gene groups
     */
    NUMBER_OF_MATCHED_GENES,

    /** the total number of CONTIGS
     */
    NUMBER_OF_CONTIGS,

    /**
     * the total number of sequences in the read database
     */
    NUMBER_OF_READS,
};


/** Hadoop application that implements the analysis pipeline for the Rumen
 * project.
 *
 * Analysis pipeline consists of executing Blast, Blat, then assembly of
 * sequence reads against a set of potential enzymes.
 * <p>
 * For more information, see rumen project home page or contact
 * <a target="_blank" href="http://www.jgi.doe.gov/">JGI</a>
 * @author Karan Bhatia
 * @author <a href="mailto:kbhatia@lbl.gov">kbhatia@lbl.gov</a>
 */

public class ReadBlaster {

    static Logger log = Logger.getLogger(ReadBlaster.class);

    /**
     * starts off the hadoop application
     *
     * @param args specify input file cassandra host and kmer size
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        /*
        load the application configuration parameters (from deployment directory)
         */
        
        Configuration conf = new Configuration();
        String[] otherArgs = MetaUtils.loadConfiguration(conf, args);

        /*
        process arguments
         */
        if (otherArgs.length != 3) {
            System.err.println("Usage: blast <readfile> <genefile> <outputdir>");
            System.exit(2);
        }

        //long sequenceFileLength = 0;

        /*
        calculate the full database size, so that parameters can be adjusted
         */
        FileSystem fs = FileSystem.get(conf);
        Path filenamePath = new Path(otherArgs[0]);
        long databaseFileSize = fs.getFileStatus(filenamePath).getLen();
        //long databaseFileSize = new File(otherArgs[0]).length(); // the overall file size

    /*    try {
            FileInputStream file = new FileInputStream(otherArgs[0]);
            BufferedReader d
                    = new BufferedReader(new InputStreamReader(file));

            String line;
            line = d.readLine();
            while (line != null) {
                if (line.charAt(0) != '>') sequenceFileLength += line.length();
                line = d.readLine();
            }
            file.close();
        } catch (Exception e) {
            System.err.println(e);
            System.exit(2);
        }*/

        conf.setLong("blast.effectivedbsize", databaseFileSize);
        conf.set("blast.genedbfilepath", otherArgs[1]);

        /*
        seems to help in file i/o performance
         */
        conf.setInt("io.file.buffer.size", 1024 * 1024);
        conf.setBoolean("debug", true);

        log.info(System.getProperty("application.name") + "[version " + System.getProperty("application.version") + "] starting with following parameters");
        log.info("\tsequence file: " + otherArgs[0]);
        log.info("\tgene db file : " + otherArgs[1]);

        String[] allProperties = {
                "--- application properties ---",
                "application.name",
                "application.version",
                "--- system properties ---",
                "mapred.min.split.size",
                "mapred.max.split.size",
                "--- blast ---",
                "blast.commandline",
                "blast.commandpath",
                "blast.tmpdir",
                "blast.cleanup",
                "blast.effectivedbsize",
                "blast.genedbfilepath",
                "blast.readsarepaired",
                "--- formatdb ---",
                "formatdb.commandpath",
                "formatdb.commandline",
                "--- blat ---",
                "blat.commandline",
                "blat.commandpath",
                "blat.tmpdir",
                "blat.cleanup",
                "blat.skipexecution",
                "blat.paired",
                "--- assembly ---",
                "assembler.command",
                "assembler.tmpdir",
                "assembler.cleanup",
                "cap3.commandline",
                "cap3.commandpath",
                "velveth.commandline",
                "velveth.commandpath",
                "velvetg.commandline",
                "velvetg.commandpath"
        };

        MetaUtils.printConfiguration(conf, log, allProperties);

        /*
        setup blast configuration parameters
         */

        boolean recalculate = false;
        boolean abort = false;

        if (!fs.exists(new Path(otherArgs[2]+"/step1"))) {
            recalculate = true;
            Job job = new Job(conf, System.getProperty("application.name")+"-"+System.getProperty("application.version")+"-step1");

            job.setJarByClass(ReadBlaster.class);
            job.setInputFormatClass(FastaBlockInputFormat.class);
            job.setMapperClass(BlastMapperGroupByGene.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IdentityReducerGroupByKey.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setNumReduceTasks(1); // force there to be only 1 to get a single output file

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]+"/step1"));

            abort = !job.waitForCompletion(true);
        }


        /*
         now setup and run blat and assembly
         */

        if (recalculate) {
            fs.delete(new Path(otherArgs[2]+"/step2"), true);
        }
        if (!abort && !fs.exists(new Path(otherArgs[2]+"/step2"))) {
            conf.set("blat.blastoutputfile", otherArgs[2]+"/step1/part-r-00000");
            Job jobBlat = new Job(conf, System.getProperty("application.name")+"-"+System.getProperty("application.version")+"-step2");
            jobBlat.setJarByClass(ReadBlaster.class);
            jobBlat.setInputFormatClass(FastaBlockInputFormat.class);
            jobBlat.setMapperClass(BlatMapperByGroup.class);
            //job.setCombinerClass(IntSumReducer.class);
            jobBlat.setReducerClass(AssembleByGroupKey.class);
            jobBlat.setOutputKeyClass(Text.class);
            jobBlat.setOutputValueClass(Text.class);
            jobBlat.setNumReduceTasks(conf.getInt("blat.numreducers", 1));

            FileInputFormat.addInputPath(jobBlat, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(jobBlat, new Path(otherArgs[2]+"/step2"));

            System.exit(jobBlat.waitForCompletion(true) ? 0 : 1);
        }
    }

}



