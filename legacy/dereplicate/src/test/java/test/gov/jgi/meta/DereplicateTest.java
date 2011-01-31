package test.gov.jgi.meta;

import gov.jgi.meta.Dereplicate;
import gov.jgi.meta.hadoop.input.FastaInputFormat;
import gov.jgi.meta.hadoop.io.ReadNode;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;

import java.io.*;

/**
 * Dereplicate Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>10/05/2010</pre>
 */
public class DereplicateTest extends TestCase {

    private MiniDFSCluster dfsCluster = null;
    private MiniMRCluster mrCluster  = null;

    private Path input;
    private Path output;


    public DereplicateTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();

        // make sure the log folder exists,
        // otherwise the test fill fail
        new File("test-logs").mkdirs();

        System.setProperty("hadoop.log.dir", "test-logs");
        System.setProperty("javax.xml.parsers.SAXParserFactory",
                "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

        Configuration conf = new Configuration();

        dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        //dfsCluster.getFileSystem().makeQualified(input);
        //dfsCluster.getFileSystem().makeQualified(output);

        mrCluster = new MiniMRCluster(1, getFileSystem().getUri().toString(), 1);


        input = new Path("target/test-classes/rep-100.fas");
        output = new Path(getFileSystem().getWorkingDirectory(), "output");

    }

    public void tearDown() throws Exception {
        super.tearDown();

        if (dfsCluster != null) {
            dfsCluster.shutdown();
            dfsCluster = null;
        }
        if (mrCluster != null) {
            mrCluster.shutdown();
            mrCluster = null;
        }

        deleteDirectory(new File("build"));
        deleteDirectory(new File("test-logs"));

    }


    public void testBlast() throws Exception {
        Configuration conf = new Configuration();

        conf.setInt("io.file.buffer.size", 1024 * 1024);
        conf.setInt("dereplicate.numreducers", 1);
        conf.setInt("dereplicate.editdistance", 0);

        int iterationNum = 0;
        int editDistance = conf.getInt("dereplicate.editdistance", 1);

        FileSystem fs = FileSystem.get(conf);
        boolean recalculate = false;

        Job job0 = new Job(conf, "dereplicate-step0");

        job0.setJarByClass(Dereplicate.class);
        job0.setInputFormatClass(FastaInputFormat.class);
        job0.setMapperClass(Dereplicate.PairMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job0.setReducerClass(Dereplicate.PairReducer.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(Text.class);
        job0.setNumReduceTasks(conf.getInt("dereplicate.numreducers", 1));

        FileInputFormat.addInputPath(job0, input);
        FileOutputFormat.setOutputPath(job0, new Path(output + "/step0"));

        job0.waitForCompletion(true);

        do {
            Job job = new Job(conf, "dereplicate-step1");

            job.setJarByClass(Dereplicate.class);
            if (iterationNum == 0) {
                job.setInputFormatClass(FastaInputFormat.class);
                job.setMapperClass(Dereplicate.GraphEdgeMapper.class);
            } else {
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapperClass(Dereplicate.GraphEdgeMapper2.class);
            }

            job.setReducerClass(Dereplicate.GraphEdgeReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(ReadNode.class);
            job.setNumReduceTasks(conf.getInt("dereplicate.numreducers", 1));

            if (iterationNum == 0) {
                FileInputFormat.addInputPath(job, new Path(output + "/step0"));
            } else {
                FileInputFormat.addInputPath(job, new Path(output + "/step1/iteration" + (iterationNum - 1)));
            }

            FileOutputFormat.setOutputPath(job, new Path(output + "/step1/iteration" + iterationNum));

            job.waitForCompletion(true);

            iterationNum++;
            
        } while (iterationNum < editDistance);

        Job job2 = new Job(conf, "dereplicate-step2");

        job2.setJarByClass(Dereplicate.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapperClass(Dereplicate.AggregateMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(Dereplicate.AggregateReducer.class);
        job2.setOutputKeyClass(ReadNode.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(conf.getInt("dereplicate.numreducers", 1));

        FileInputFormat.addInputPath(job2, new Path(output + "/step1/iteration" + (iterationNum - 1)));
        FileOutputFormat.setOutputPath(job2, new Path(output + "/step2"));


        job2.waitForCompletion(true);


        Job job3 = new Job(conf, "dereplicate-step3");
        job3.setJarByClass(Dereplicate.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setMapperClass(Dereplicate.ChooseMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job3.setReducerClass(Dereplicate.ChooseReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(ReadNode.class);
        job3.setNumReduceTasks(conf.getInt("dereplicate.numreducers", 1));

        FileInputFormat.addInputPath(job3, new Path(output + "/step2"));
        FileOutputFormat.setOutputPath(job3, new Path(output + "/step3"));

        job3.waitForCompletion(true);

        // check the output
        Path outputFile = new Path(output+"/step3", "part-r-00000");
        InputStream is     = getFileSystem ().open(outputFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        Assert.assertEquals(">10.-1036608916 numberOfReads=10\t", reader.readLine());
        Assert.assertEquals("ccacgaaatatcacacagtctgtgcaggctattctgcggctttcagtcttttgtgcggtaacgaggaccttgaggccggagttccaggagcaatggcgtgttttctgactttcggcgtcttatcggacgtgtctgacacactataaagcacc",
                reader.readLine());
        int linecount = 2;
        while (reader.readLine() != null) { linecount++; }
        Assert.assertEquals(linecount, 200);

        reader.close();

    }

    protected FileSystem getFileSystem() throws IOException
    {
       return(dfsCluster.getFileSystem());
    }

      static public boolean deleteDirectory(File path) {
    if( path.exists() ) {
      File[] files = path.listFiles();
      for(int i=0; i<files.length; i++) {
         if(files[i].isDirectory()) {
           deleteDirectory(files[i]);
         }
         else {
           files[i].delete();
         }
      }
    }
    return( path.delete() );
  }


    public static Test suite() {
        return new TestSuite(DereplicateTest.class);
    }
}
