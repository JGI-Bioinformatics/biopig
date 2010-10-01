

package test.gov.jgi.meta.hadoop.apps;


import gov.jgi.meta.hadoop.apps.Fastq2FastaApp;
import gov.jgi.meta.hadoop.input.FastqInputFormat;
import gov.jgi.meta.hadoop.map.FastaIdentityMapper;
import gov.jgi.meta.hadoop.output.FastaOutputFormat;
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
 * Fastq2FastaApp Tester.
 *
 * @author Karan Bhatia
 * @since <pre>09/27/2010</pre>
 * @version 1.0
 */
public class Fastq2FastaAppTest extends TestCase {
   private MiniDFSCluster dfsCluster = null;
   private MiniMRCluster mrCluster  = null;

   private Path input;
   private Path output;

   public Fastq2FastaAppTest(String name)
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
   public void testMain() throws Exception
   {
      Configuration conf = new Configuration();

      input = createTextInputFile();
      output = new Path(getFileSystem().getWorkingDirectory(), "output");

      Job job0 = new Job(conf, "fastq2fasta");
      job0.setJarByClass(Fastq2FastaApp.class );
      job0.setInputFormatClass(FastqInputFormat.class );
      job0.setMapperClass(FastaIdentityMapper.class );
      job0.setOutputKeyClass(Text.class );
      job0.setOutputValueClass(Text.class );
      job0.setOutputFormatClass(FastaOutputFormat.class );
      job0.setNumReduceTasks(0);

      FileInputFormat.addInputPath(job0, input);   // this is the reads file
      FileOutputFormat.setOutputPath(job0, output);

      job0.waitForCompletion(true);

      // check the output
      FileStatus[] fs = getFileSystem().listStatus(output);
      Path[] outputFiles = FileUtil.stat2Paths(fs);        

      Assert.assertEquals(1, outputFiles.length);
      InputStream is     = getFileSystem().open(outputFiles[0]);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String x = reader.readLine();
      Assert.assertEquals(">SEQ_ID", x);
      String y = reader.readLine();
      Assert.assertEquals("gatttggggttcaaagcagtatcgatcaaatagtaaatccatttgttcaactcacagttt", y);
       Assert.assertNull(reader.readLine());
      reader.close();
   }

   public static Test suite()
   {
      return(new TestSuite(Fastq2FastaAppTest.class ));
   }

   protected FileSystem getFileSystem() throws IOException
   {
      return(dfsCluster.getFileSystem());
   }

   private Path createTextInputFile() throws IOException
   {
      Path x = new Path(getFileSystem().getWorkingDirectory(), "wordcount");
      OutputStream os = getFileSystem().create(x);
      Writer       wr = new OutputStreamWriter(os);

      wr.write("@SEQ_ID\n" +
              "GATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT\n" +
              "+\n" +
              "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65");
      wr.close();

      return x;
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
