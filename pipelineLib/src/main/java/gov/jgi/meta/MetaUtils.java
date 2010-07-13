package gov.jgi.meta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Enumeration;
import java.util.Properties;

/** utility class for common functionality across various applications
 *
 * @author karan bhatia
 */
public class MetaUtils {

    public static String[] loadConfiguration(Configuration conf, String[] args)
    {
        /*
        first load the configuration from the build properties (typically packaged in the jar)
         */
        System.out.println("loading build.properties ...");
        try {
            Properties buildProperties = new Properties();
            buildProperties.load(ClassLoader.getSystemResource("build.properties").openStream());
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
        String appName = System.getProperty("application.name");
        String appVersion = System.getProperty("application.version");
        String confFileName = appName+"-"+appVersion+"-conf.xml";
        System.out.println("loading application configuration from " + confFileName);
        conf.addResource(confFileName);

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
}
