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

import gov.jgi.meta.hadoop.input.FastaBlockLineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * utility class for common functionality across various applications
 *
 * @author karan bhatia
 */
public class MetaUtils {

   /**
    * parses commandline args and configures the hadoop job configuration with defaults loaded from
    * either cluster defaults file ($(META_HOME)/conf/<app>-conf.xml), user defaults (~/.meta-prefs)
    * or commandline args (with -D<parameter>=<value>).  The <app> by default is of the form:
    * <application.name>HadoopApp-conf.xml
    *
    * @param conf the job configuration to add the defaults to
    * @param args the commandline args
    * @return modifies conf with appropriate defaults
    */
   public static String[] loadConfiguration(Configuration conf, String[] args)
   {
      return(loadConfiguration(conf, null, args));
   }

   /**
    * same as {@link #loadConfiguration(Configuration, String[]) loadConfiguration()} but loads from
    * a specified file instead of the default filename.
    *
    * @param conf the job configuration to add the defaults to
    * @param configurationFileName the cluster defaults file to load
    * @param args the commandline args
    * @return modifies conf
    */
   public static String[] loadConfiguration(Configuration conf, String configurationFileName, String[] args)
   {
      /*
       * first load the configuration from the build properties (typically packaged in the jar)
       */
      System.out.println("loading build.properties ...");
      try {
         Properties buildProperties = new Properties();
         buildProperties.load(MetaUtils.class .getResourceAsStream("/build.properties"));
         for (Enumeration e = buildProperties.propertyNames(); e.hasMoreElements();)
         {
            String k = (String)e.nextElement();
            System.out.println("setting " + k + " to " + buildProperties.getProperty(k));
            System.setProperty(k, buildProperties.getProperty(k));
            conf.set(k, buildProperties.getProperty(k));
         }
      } catch (Exception e) {
         System.out.println("unable to find build.properties ... skipping");
      }

      /*
       * override properties with the deployment descriptor
       */
      if (configurationFileName == null)
      {
         String appName    = System.getProperty("application.name");
         String appVersion = System.getProperty("application.version");
         configurationFileName = appName + "-" + appVersion + "-conf.xml";
      }
      System.out.println("loading application configuration from " + configurationFileName);
      try {
         URL u = ClassLoader.getSystemResource(configurationFileName);
         if (u == null)
         {
            System.err.println("unable to find " + configurationFileName + " ... skipping");
         }
         else
         {
            conf.addResource(configurationFileName);
         }
      } catch (Exception e) {
         System.err.println("unable to find " + configurationFileName + " ... skipping");
      }

      /*
       * override properties from user's preferences defined in ~/.meta-prefs
       */

      try {
         java.io.FileInputStream fis = new java.io.FileInputStream(new java.io.File(System.getenv("HOME") + "/.meta-prefs"));
         Properties props            = new Properties();
         props.load(fis);
         System.out.println("loading preferences from ~/.meta-prefs");
         for (Enumeration e = props.propertyNames(); e.hasMoreElements();)
         {
            String k = (String)e.nextElement();
            System.out.println("overriding property: " + k);
            conf.set(k, props.getProperty(k));
         }
      } catch (Exception e) {
         System.out.println("unable to find ~/.meta-prefs ... skipping");
      }

      /*
       * finally, allow user to override from commandline
       */
      return(new GenericOptionsParser(conf, args).getRemainingArgs());
   }

   /**
    * prints out the configuration properties
    * @param conf the job configuration holding parameter values
    * @param log the logger to use to print the information
    * @param allProperties a string array with the set of properties to print
    */
   public static void printConfiguration(Configuration conf, Logger log, String[] allProperties)
   {
      for (String option : allProperties)
      {
         if (option.startsWith("---"))
         {
            log.info(option);
            continue;
         }
         String c = conf.get(option);
         if (c != null)
         {
            log.info("\toption " + option + ":\t" + c);
         }
      }
   }

   /**
    * same as {@link #printConfiguration(Configuration, Logger, String[]) printConfiguration(Configuration, Logger, String[])}
    * but prints to stdout.
    * @param conf the job configuration holding the values
    * @param allProperties a string array of values
    */
   public static void printConfiguration(Configuration conf, String[] allProperties)
    {
       for (String option : allProperties)
       {
          if (option.startsWith("---"))
          {
             System.out.println(option);
             continue;
          }
          String c = conf.get(option);
          if (c != null)
          {
             System.out.println("\toption " + option + ":\t" + c);
          }
       }
    }

   /**
    * find all files from a given root.
    * @param p is the root on which to search
    * @return a set of file paths
    * @throws IOException if filesystem can't find file or has other io problems
    */
   public static Set<Path> findAllPaths(Path p) throws IOException
   {
      Configuration conf = new Configuration();
      FileSystem    fs   = FileSystem.get(conf);

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

   /**
    * counts the number of sequences in fasta format found in given file or directory
    * @param contigFileName the file or directory name
    * @return an integer count of the number of sequences found
    * @throws IOException if filesystem has error
    */
   public static int countSequences(String contigFileName) throws IOException
   {
      return(countSequences(contigFileName, new Configuration()));
   }

   /**
    * see @{link #countSequences(String) countSequences(String)}
    * @param contigFileName the file or directory name
    * @param conf the hadoop configuration object specifiing filesystem
    * @return an integer count of number of sequences found
    * @throws IOException if filesystem has error
    */
   public static int countSequences(String contigFileName, Configuration conf) throws IOException
   {
      FileSystem fs           = FileSystem.get(conf);
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

   /**
    * reads a fasta file and returns a map with id and sequence contents
    * @param contigFileName the file or file path of the fasta file
    * @return map containing sequences keyed by sequence id
    * @throws IOException if file exists
    */
   public static Map<String, String> readSequences(String contigFileName) throws IOException
   {
      Configuration conf         = new Configuration();
      FileSystem    fs           = FileSystem.get(conf);
      Path          filenamePath = new Path(contigFileName);

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
         fblr.close();
      }

      return(results);
   }

   /**
    * writes sequences from map to fasta file
    *
    * @param seqList is the map of sequences indexed by sequence id
    * @param filename the name/path of the file to create
    * @return the full path of the location of the fasta file
    * @throws IOException if file already exists
    */
   public static String sequenceToFile(Map<String, String> seqList, String filename) throws IOException
   {
      Configuration conf = new Configuration();
      FileSystem    fs   = FileSystem.get(conf);
      Path          fp   = new Path(filename);

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

      /**
    * given a map of sequences indexed by sequence id, write out a fasta file to local file system.
    * files are created with rwx bits set to 777.
    *
    * @param seqList is the list of sequences to create the database with
    * @param dirName the directory path in which to create the file
    * @param tmpFileName the file name to create
    * @return the full path of the location of the database
    * @throws IOException if error occures in file creation
    */
   public static String sequenceToLocalFile(Map<String, String> seqList, String dirName, String tmpFileName) throws IOException
   {
      return sequenceToLocalFile(seqList, new Path(dirName, tmpFileName).toString());
   }
   /**
    * given a map of sequences indexed by sequence id, write out a fasta file to local file system.
    * files are created with rwx bits set to 777.
    *
    * @param seqList is the list of sequences to create the database with
    * @param tmpFileName the file name/path to create
    * @return the full path of the location of the database
    * @throws IOException if error occures in file creation
    */
   public static String sequenceToLocalFile(Map<String, String> seqList, String tmpFileName) throws IOException
   {
      BufferedWriter out;
      File           seqFile = null;

      /*
       * open temp file
       */
      seqFile = new File(tmpFileName);

      if (! ( seqFile.setExecutable(true, false) &&
              seqFile.setReadable(true, false) &&
              seqFile.setWritable(true, false) ) ) {
         throw new IOException("unable to set RWX bits to 777");
      }
      out = new BufferedWriter(new FileWriter(seqFile.getPath()));

      /*
       * write out the sequences to file
       */
      for (String key : seqList.keySet())
      {
         assert(seqList.get(key) != null);
         out.write(">" + key + "\n");
         out.write(seqList.get(key) + "\n");
      }

      /*
       * close temp file
       */
      out.close();

      return(seqFile.getPath());
   }

   /**
    * Create a new temporary directory. Use something like
    * {@link #recursiveDelete(java.io.File)} to clean this directory up since it isn't
    * deleted automatically
    *
    * @param tmpDir is the directory underwhich to create the tmp dir
    * @return the new directory
    * @throws java.io.IOException if there is an error creating the temporary directory
    */
   public static File createTempDir(String tmpDir) throws IOException
   {
      final File sysTempDir = new File(tmpDir);
      File       newTempDir;
      final int  maxAttempts  = 9;
      int        attemptCount = 0;

      do
      {
         attemptCount++;
         if (attemptCount > maxAttempts)
         {
            throw new IOException(
               "The highly improbable has occurred! Failed to " +
               "create a unique temporary directory after " +
               maxAttempts + " attempts.");
         }
         String dirName = UUID.randomUUID().toString();
         newTempDir = new File(sysTempDir, dirName);
      } while (newTempDir.exists());

      if (newTempDir.mkdirs())
      {
         if (! ( newTempDir.setExecutable(true, false) &&
                 newTempDir.setReadable(true, false) &&
                 newTempDir.setWritable(true, false) ) ) {
            throw new IOException("unable to set RWX bits to 777");
         }
         return(newTempDir);
      }
      else
      {
         throw new IOException("Failed to create temp dir named " + newTempDir.getAbsolutePath());
      }
   }

   /**
    * Recursively delete file or directory
    *
    * @param fileOrDir the file or dir to delete
    * @return true iff all files are successfully deleted
    */
   public static boolean recursiveDelete(File fileOrDir)
   {
      if (fileOrDir.isDirectory())
      {
         // recursively delete contents
         for (File innerFile : fileOrDir.listFiles())
         {
            if (!recursiveDelete(innerFile))
            {
               fileOrDir.deleteOnExit();
               return(false);
            }
         }
      }
      if (!fileOrDir.delete())
      {
         fileOrDir.deleteOnExit();
         return(false);
      }
      return(true);
   }


   /**
    * given a sequence, return the reverse complement
    * @param s sequence
    * @return its reverse complement
    */
   public static String reverseComplement(String s)
   {
      StringBuffer sb = new StringBuffer();

      for (int i = 0; i < s.length(); i++)
      {
         if (s.charAt(i) == 'a') { sb.append("t"); }
         else if (s.charAt(i) == 't') { sb.append("a"); }
         else if (s.charAt(i) == 'g') { sb.append("c"); }
         else if (s.charAt(i) == 'c') { sb.append("g"); }
         else if (s.charAt(i) == 'n') { sb.append("n"); }
      }
      return(sb.reverse().toString());
   }

   /**
    * find all sequences that are within distance edit distance
    * @param start sequence to start with
    * @param distance edit distance
    * @return set of sequences that are within distance including start sequence
    */
   public static Set<String> generateAllNeighbors(String start, int distance)
   {
      Set<String> neighbors = generateAllNeighbors(start, distance, new HashSet());
      neighbors.add(start);
      return(neighbors);
   }

   public static Set<String> generateAllNeighbors2(String start, int distance)
   {
      Set<String> r = new HashSet<String>();

      if (distance == 0)
      {
         r.add(start);
         return(r);
      }
      else if (distance == 1)
      {
         return(generateHammingDistanceOne(start));
      }
      else if (distance == 2)
      {
         return(generateHammingDistanceTwo(start));
      }
      else
      {
         // throw exception;
      }
      return(r);
   }

   private static Set<String> generateHammingDistanceOne(String start)
   {
      char[] bases = { 'a', 't', 'g', 'c', 'n' };
      Set<String> r = new HashSet<String>();

      for (int i = 0; i < start.length(); i++)
      {
         for (char basePair : bases)
         {
            if (start.charAt(i) == basePair) { continue; }
            String n = stringReplaceIth(start, i, basePair);
            if (r.contains(n)) { continue; }
            r.add(n);
         }
      }
      return(r);
   }

   private static Set<String> generateHammingDistanceTwo(String start)
   {
      byte[] b     = start.getBytes();
      byte[] bases = { 'a', 't', 'g', 'c', 'n' };
      Set<String> r = new HashSet<String>();

      for (int i = 0; i < start.length() - 1; i++)
      {
         for (int j = i + 1; j < start.length(); j++)
         {
            byte ii = b[i];
            byte jj = b[j];
            for (byte basePair1 : bases)
            {
               for (byte basePair2 : bases)
               {
                  b[i] = basePair1;
                  b[j] = basePair2;
                  r.add(b.toString());
               }
            }
            b[i] = ii;
            b[j] = jj;
         }
      }
      return(r);
   }

   public static Set<String> generateAllNeighbors(String start, int distance, Set x)
   {
      char [] bases = { 'a', 't', 'g', 'c', 'n' };
      Set<String> s = new HashSet<String>();

      //s.add(start);
      if (distance == 0)
      {
         return(s);
      }

      for (int i = 0; i < start.length(); i++)
      {
         for (char basePair : bases)
         {
            if (start.charAt(i) == basePair) { continue; }
            String n = stringReplaceIth(start, i, basePair);
            if (x.contains(n)) { continue; }

            s.add(n);
            s.addAll(generateAllNeighbors(n, distance - 1, s));
         }
      }

      return(s);
   }


   /**
    * replace the i'th character of string s with character c
    * @param s the string
    * @param i the index of the string to replace
    * @param c the character to replace
    * @return the new string
    * @throws IndexOutOfBoundsException if necessary
    */
   public static String stringReplaceIth(String s, int i, char c) throws IndexOutOfBoundsException
   {
      if (i >= s.length()) {
         throw new IndexOutOfBoundsException("index " + i + " greater than length of " + s);
      }

      return(s.substring(0, i) + c + s.substring(i + 1));
   }

   public static void configureLog4j()
   {
      // configure the log4j system for hadoop map jobs

      String log4jConfigurationFile = System.getProperty("log4j.properties");

      System.out.println("configuring log4j using: " + log4jConfigurationFile);

      URL u = ClassLoader.getSystemResource(log4jConfigurationFile);
      if (u == null)
      {
         System.err.println("unable to find " + log4jConfigurationFile + " ... skipping");
      }
      else
      {
         PropertyConfigurator.configure(log4jConfigurationFile);
      }
   }
}
