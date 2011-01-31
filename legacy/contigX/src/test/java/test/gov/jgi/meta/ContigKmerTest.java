package test.gov.jgi.meta;

import gov.jgi.meta.MetaUtils;
import gov.jgi.meta.ContigKmer;
import gov.jgi.meta.hadoop.input.FastaBlockLineReader;
import gov.jgi.meta.hadoop.input.FastaInputFormat;
import gov.jgi.meta.hadoop.reduce.AssembleByGroupKey;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;

import java.io.*;
import java.io.File;
import java.util.*;

/**
 * ContigKmer Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>10/27/2010</pre>
 */
public class ContigKmerTest extends TestCase {
   private MiniDFSCluster dfsCluster = null;
   private MiniMRCluster  mrCluster  = null;
   Configuration          conf;

   Path output;

   public ContigKmerTest(String name)
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

      conf = new Configuration();

      dfsCluster = new MiniDFSCluster(conf, 1, true, null);
      //dfsCluster.getFileSystem().makeQualified(input);
      //dfsCluster.getFileSystem().makeQualified(output);

      mrCluster = new MiniMRCluster(1, getFileSystem().getUri().toString(), 1);

      output = new Path(getFileSystem().getWorkingDirectory(), "output");
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
    * Method: main(String[] args)
    */
   public void testWithCap3() throws Exception
   {
      Configuration conf2 = new Configuration();

      String[] otherArgs = MetaUtils.loadConfiguration(conf2, "test-config.xml", null);

      Job job = new Job(conf2, "contigkmertestwithcap3");

      Path inputContigsFileOrDir  = new Path("target/test-classes/start_76l_o50.fa");
      Map<String, String> results = new TreeMap<String, String>();
      results.putAll(MetaUtils.readSequences("target/test-classes/shredded_76l_o50.fa"));
      int numContigs         = results.size();
      int iteration          = 0;
      int numberOfIterations = 1;
      do
      {
         iteration++;
         System.out.println(" *******   iteration " + iteration + "   ********");

         // check to see if output already exists.

         Path outputContigFileName = new Path(output, "contigs-" + iteration + ".fas");
         Path outputContigDirName  = new Path(output, "step" + iteration);

         conf2.set("contigfilename", inputContigsFileOrDir.toString());

         Job job0 = new Job(conf2, "configkmer: " + "iteration " + iteration + ", file = " + inputContigsFileOrDir);
         job0.setJarByClass(ContigKmer.class );
         job0.setInputFormatClass(FastaInputFormat.class );
         job0.setMapperClass(ContigKmer.ContigKmerMapper.class );
         //job.setCombinerClass(IntSumReducer.class);
         job0.setReducerClass(AssembleByGroupKey.class );
         //job0.setReducerClass(IdentityReducerGroupByKey.class);
         job0.setOutputKeyClass(Text.class );
         job0.setOutputValueClass(Text.class );
         job0.setNumReduceTasks(conf.getInt("contigkmer.numreducers", 1));

         FileInputFormat.addInputPath(job0, new Path("target/test-classes/shredded_76l_o50.fa"));      // this is the reads file
         FileOutputFormat.setOutputPath(job0, outputContigDirName);

         job0.waitForCompletion(true);

         numContigs = countSequences(outputContigDirName.toString());
         Map<String, String> tmpresults = readSequences(outputContigDirName.toString());
         for (String k : tmpresults.keySet())
         {
            String[] a = k.split("-", 2);
            results.put(a[0], tmpresults.get(k));
         }

         try {
            sequenceToFile(results, outputContigFileName.toString());
         }
         catch (IOException e) {
            System.out.println(e);
            System.out.println("continuing");
         }

         inputContigsFileOrDir = outputContigDirName;
      } while (iteration < numberOfIterations && numContigs > 0);

      Path           outputFile = new Path(output, "contigs-1.fas");
      InputStream    is         = getFileSystem().open(outputFile);
      BufferedReader reader     = new BufferedReader(new InputStreamReader(is));
      String         x          = reader.readLine();
      Assert.assertEquals(">ref_NC_001133_(0..76) length=107", x);
      reader.close();
   }

   public void testWithMinimus() throws Exception
   {
      Configuration conf2 = new Configuration();

      String[] otherArgs = MetaUtils.loadConfiguration(conf2, "test-config.xml", null);
      conf2.set("assembler.command", "minimus");
      Job job = new Job(conf2, "contigkmertestwithminimus");

      Path inputContigsFileOrDir  = new Path("target/test-classes/start_76l_o50.fa");
      Map<String, String> results = new TreeMap<String, String>();
      results.putAll(MetaUtils.readSequences("target/test-classes/shredded_76l_o50.fa"));
      int numContigs         = results.size();
      int iteration          = 0;
      int numberOfIterations = 1;
      do
      {
         iteration++;
         System.out.println(" *******   iteration " + iteration + "   ********");

         // check to see if output already exists.

         Path outputContigFileName = new Path(output, "contigs-" + iteration + ".fas");
         Path outputContigDirName  = new Path(output, "step" + iteration);

         conf2.set("contigfilename", inputContigsFileOrDir.toString());

         Job job0 = new Job(conf2, "configkmer: " + "iteration " + iteration + ", file = " + inputContigsFileOrDir);
         job0.setJarByClass(ContigKmer.class );
         job0.setInputFormatClass(FastaInputFormat.class );
         job0.setMapperClass(ContigKmer.ContigKmerMapper.class );
         //job.setCombinerClass(IntSumReducer.class);
         job0.setReducerClass(AssembleByGroupKey.class );
         //job0.setReducerClass(IdentityReducerGroupByKey.class);
         job0.setOutputKeyClass(Text.class );
         job0.setOutputValueClass(Text.class );
         job0.setNumReduceTasks(conf.getInt("contigkmer.numreducers", 1));

         FileInputFormat.addInputPath(job0, new Path("target/test-classes/shredded_76l_o50.fa"));      // this is the reads file
         FileOutputFormat.setOutputPath(job0, outputContigDirName);

         job0.waitForCompletion(true);

         numContigs = countSequences(outputContigDirName.toString());
         Map<String, String> tmpresults = readSequences(outputContigDirName.toString());
         for (String k : tmpresults.keySet())
         {
            String[] a = k.split("-", 2);
            results.put(a[0], tmpresults.get(k));
         }

         try {
            sequenceToFile(results, outputContigFileName.toString());
         }
         catch (IOException e) {
            System.out.println(e);
            System.out.println("continuing");
         }

         inputContigsFileOrDir = outputContigDirName;
      } while (iteration < numberOfIterations && numContigs > 0);

      Path           outputFile = new Path(output, "contigs-1.fas");
      InputStream    is         = getFileSystem().open(outputFile);
      BufferedReader reader     = new BufferedReader(new InputStreamReader(is));
      String         x          = reader.readLine();
      Assert.assertEquals(">ref_NC_001133_(0..76) length=107", x);
      reader.close();
   }

   protected FileSystem getFileSystem() throws IOException
   {
      return(dfsCluster.getFileSystem());
   }

   private String sequenceToFile(Map<String, String> seqList, String filename) throws IOException
   {
      FileSystem fs = getFileSystem();
      Path       fp = new Path(filename);

      if (fs.exists(fp))
      {
         throw new IOException("file " + filename + " already exists");
      }

      FSDataOutputStream out = fs.create(fp);

      /*
       * write out the sequences to file
       */
      for (String key : seqList.keySet())
      {
         assert(seqList.get(key) != null);
         out.writeBytes(">" + key + " length=" + seqList.get(key).length() + "\n");
         out.writeBytes(seqList.get(key) + "\n");
      }

      /*
       * close temp file
       */
      out.close();


      return(fp.toString());
   }

   private Map<String, String> readSequences(String contigFileName) throws IOException
   {
      FileSystem fs           = getFileSystem();
      Path       filenamePath = new Path(contigFileName);

      Map<String, String> results = new HashMap<String, String>();

      if (!fs.exists(filenamePath))
      {
         throw new IOException("file not found: " + contigFileName);
      }

      for (Path f : findAllPaths(filenamePath))
      {
         FSDataInputStream    in   = fs.open(f);
         FastaBlockLineReader fblr = new FastaBlockLineReader(in);

         Text key    = new Text();
         long length = fs.getFileStatus(f).getLen();
         HashMap<String, String> tmpcontigs = new HashMap<String, String>();
         fblr.readLine(key, tmpcontigs, Integer.MAX_VALUE, (int)length);
         results.putAll(tmpcontigs);
         in.close();
      }

      return(results);
   }

   private int countSequences(String contigFileName) throws IOException
   {
      FileSystem fs           = getFileSystem();
      Path       filenamePath = new Path(contigFileName);
      int        count        = 0;

      if (!fs.exists(filenamePath))
      {
         throw new IOException("file not found: " + contigFileName);
      }

      for (Path f : findAllPaths(filenamePath))
      {
         FSDataInputStream    in   = fs.open(f);
         FastaBlockLineReader fblr = new FastaBlockLineReader(in);

         Text key    = new Text();
         long length = fs.getFileStatus(f).getLen();
         HashMap<String, String> tmpcontigs = new HashMap<String, String>();
         fblr.readLine(key, tmpcontigs, Integer.MAX_VALUE, (int)length);
         count += tmpcontigs.size();
         in.close();
      }

      return(count);
   }

   private Set<Path> findAllPaths(Path p) throws IOException
   {
      FileSystem fs = getFileSystem();

      HashSet<Path> s = new HashSet<Path>();

      if (fs.getFileStatus(p).isDir())
      {
         for (FileStatus f : fs.listStatus(p))
         {
            if (!f.isDir())
            {
               s.add(f.getPath());
            }
         }
      }
      else
      {
         s.add(p);
      }

      return(s);
   }

   static public boolean deleteDirectory(File path)
   {
      if (path.exists())
      {
         File[] files = path.listFiles();
         for (int i = 0; i < files.length; i++)
         {
            if (files[i].isDirectory())
            {
               deleteDirectory(files[i]);
            }
            else
            {
               files[i].delete();
            }
         }
      }
      return(path.delete());
   }

   public static Test suite()
   {
      return(new TestSuite(ContigKmerTest.class ));
   }
}
