package gov.jgi.meta;

import gov.jgi.meta.hadoop.input.FastaBlockLineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.util.*;

/** utility class for common functionality across various applications
 *
 * @author karan bhatia
 */
public class MetaUtils {

    public static String[] loadConfiguration(Configuration conf, String[] args) {

        String appName = System.getProperty("application.name");
        String appVersion = System.getProperty("application.version");
        String confFileName = appName+"-"+appVersion+"-conf.xml";

        return loadConfiguration(conf, confFileName, args);

    }

    public static String[] loadConfiguration(Configuration conf, String configurationFileName, String[] args)
    {
        /*
        first load the configuration from the build properties (typically packaged in the jar)
         */
        System.out.println("loading build.properties ...");
        try {
            Properties buildProperties = new Properties();
            buildProperties.load(MetaUtils.class.getResourceAsStream("/build.properties"));
            for (Enumeration e = buildProperties.propertyNames(); e.hasMoreElements() ;) {
                String k = (String) e.nextElement();
                System.out.println("setting " + k + " to " + buildProperties.getProperty(k));
                System.setProperty(k, buildProperties.getProperty(k));
                conf.set(k, buildProperties.getProperty(k));
            }

        } catch (Exception e) {
            System.out.println("unable to find build.properties ... skipping");
        }

        /*
        override properties with the deployment descriptor
         */

        System.out.println("loading application configuration from " + configurationFileName);
        try {

            URL u = ClassLoader.getSystemResource(configurationFileName);
            System.out.println("url = " + u);
            conf.addResource(configurationFileName);
        } catch (Exception e) {
            System.out.println("unable to find " + configurationFileName + " ... skipping");
        }

        /*
        override properties from user's preferences defined in ~/.meta-prefs
         */

        try {
            java.io.FileInputStream fis = new java.io.FileInputStream(new java.io.File(System.getenv("HOME") + "/.meta-prefs"));
            Properties props = new Properties();
            props.load(fis);
            System.out.println("loading preferences from ~/.meta-prefs");
            for (Enumeration e = props.propertyNames(); e.hasMoreElements() ;) {
                String k = (String) e.nextElement();
                System.out.println("overriding property: " + k);
                conf.set(k, props.getProperty(k));
            }
        } catch (Exception e) {
            System.out.println("unable to find ~/.meta-prefs ... skipping");
        }

        /*
        finally, allow user to override from commandline
         */
        return new GenericOptionsParser(conf, args).getRemainingArgs();

    }


    public static void printConfiguration(Configuration conf, Logger log, String[] allProperties) {

        for (String option : allProperties) {

            if (option.startsWith("---")) {
                log.info(option);
                continue;
            }
            String c = conf.get(option);
            if (c != null) {
                log.info("\toption " + option + ":\t" + c);
            }
        }
    }

    public static Set<Path> findAllPaths(Path p) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        HashSet<Path> s = new HashSet<Path>();

        if (fs.getFileStatus(p).isDir()) {

            for (FileStatus f : fs.listStatus(p)) {

                if (!f.isDir()) {
                    s.add(f.getPath());
                }

            }

        } else {

            s.add(p);

        }

        return s;
    }

    public static int countSequences(String contigFileName) throws IOException {

         Configuration conf = new Configuration();
         FileSystem fs = FileSystem.get(conf);
         Path filenamePath = new Path(contigFileName);
         int count = 0;

         if (!fs.exists(filenamePath)) {
             throw new IOException("file not found: " + contigFileName);
         }

         for (Path f : findAllPaths(filenamePath)) {

             FSDataInputStream in = fs.open(f);
             FastaBlockLineReader fblr = new FastaBlockLineReader(in);

             Text key = new Text();
             long length = fs.getFileStatus(f).getLen();
             HashMap<String, String> tmpcontigs = new HashMap<String, String>();
             fblr.readLine(key, tmpcontigs, Integer.MAX_VALUE, (int) length);
             count += tmpcontigs.size();
             in.close();
         }

         return count;
     }

}
