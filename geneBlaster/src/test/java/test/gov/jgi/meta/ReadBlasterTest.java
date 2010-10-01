

package test.gov.jgi.meta;


import gov.jgi.meta.ReadBlaster;
import gov.jgi.meta.hadoop.apps.Fastq2FastaApp;
import gov.jgi.meta.hadoop.input.FastaBlockInputFormat;
import gov.jgi.meta.hadoop.input.FastqInputFormat;
import gov.jgi.meta.hadoop.map.BlastMapperGroupByGene;
import gov.jgi.meta.hadoop.map.BlatMapperByGroup;
import gov.jgi.meta.hadoop.map.FastaIdentityMapper;
import gov.jgi.meta.hadoop.output.FastaOutputFormat;
import gov.jgi.meta.hadoop.reduce.AssembleByGroupKey;
import gov.jgi.meta.hadoop.reduce.IdentityReducerGroupByKey;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;

import java.io.*;


/**
 * ReadBlaster App Tester.
 *
 * @author Karan Bhatia
 * @since <pre>09/27/2010</pre>
 * @version 1.0
 */
public class ReadBlasterTest extends TestCase {
   private MiniDFSCluster dfsCluster = null;
   private MiniMRCluster mrCluster  = null;

   private Path input;
   private Path output;
   private Path output2;

   public ReadBlasterTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
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


      input = new Path("target/test-classes/1M.fas");
      output = new Path(getFileSystem().getWorkingDirectory(), "output");
      output2 = new Path(getFileSystem().getWorkingDirectory(), "output2");

   }

   public void tearDown() throws Exception
   {
      super.tearDown();

      if (dfsCluster != null)
      {
         dfsCluster.shutdown();
         dfsCluster = null;
      }
      if (mrCluster != null)
      {
         mrCluster.shutdown();
         mrCluster = null;
      }

      deleteDirectory(new File("build"));
      deleteDirectory(new File("test-logs"));

   }

   /**
    *
    * Method: main(String[] args)
    *
    */
   public void testBlast() throws Exception
   {
      Configuration conf = new Configuration();

      conf.set("blast.genedbfilepath", "target/test-classes/EC.faa");

      Job job = new Job(conf, "readblastertest");

      job.setJarByClass(ReadBlaster.class);
      job.setInputFormatClass(FastaBlockInputFormat.class);
      job.setMapperClass(BlastMapperGroupByGene.class);

      job.setReducerClass(IdentityReducerGroupByKey.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(job, input);   // this is the reads file
      FileOutputFormat.setOutputPath(job, output);

      job.waitForCompletion(true);

      // check the output
      Path outputFile = new Path(output,"part-r-00000");

      InputStream is     = getFileSystem().open(outputFile);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String x = reader.readLine();
      Assert.assertEquals("AAG59608.1\t756:1:1:1194:19446\t756:1:1:1209:19648\t756:1:1:1299:18106\t756:1:1:1087:14069\t756:1:1:1324:5196\t756:1:1:1466:12924\t756:1:1:1542:16898\t756:1:1:1403:13594\t756:1:1:1281:4762\t756:1:1:1416:6763\t756:1:1:1436:10778\t756:1:1:1194:19446\t756:1:1:1110:15191", x);
      Assert.assertNull(reader.readLine());
      reader.close();
   }

   public void testBlatAndAssembly() throws Exception
   {

      Configuration conf = new Configuration();

      conf.set("blat.blastoutputfile", "target/test-classes/blastoutput");

      Job job = new Job(conf, "readblastertest-blat");

      job.setJarByClass(ReadBlaster.class);
      job.setInputFormatClass(FastaBlockInputFormat.class);
      job.setMapperClass(BlatMapperByGroup.class);
      //job.setCombinerClass(IntSumReducer.class);
      job.setReducerClass(AssembleByGroupKey.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(conf.getInt("blat.numreducers", 1));

      FileInputFormat.addInputPath(job, input);
      FileOutputFormat.setOutputPath(job, output2);

      job.waitForCompletion(true);

      // check the output
      Path outputFile = new Path(output2, "part-r-00000");

      InputStream is     = getFileSystem ().open(outputFile);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String x = reader.readLine();
      Assert.assertEquals(">AAG59608.1-NODE_2_length_85_cov_1.317647 numberOfReadsInput=24 \t", x);
      reader.close();
   }


   public static Test suite()
   {
      return(new TestSuite(ReadBlasterTest.class ));
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

}
