package gov.jgi.meta.hadoop.reduce;

import gov.jgi.meta.exec.CapCommand;
import gov.jgi.meta.exec.CommandLineProgram;
import gov.jgi.meta.exec.VelvetCommand;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * simple reducer that just outputs the matches grouped by gene
 */
public class AssembleByGroupKey extends Reducer<Text, Text, Text, Text> {
    private IntWritable result = new IntWritable();

    static Logger log = Logger.getLogger(AssembleByGroupKey.class);

    /**
     * blast command wrapper
     */
    CommandLineProgram assemblerCmd = null;

    Boolean assemblerKeepTopHitOnly = false;
    Boolean assemblerFilterBySizeSpecial = false;
    Boolean assemblerRemoveIdenticalSequences = false;

    /**
     * initialization of mapper retrieves connection parameters from context and opens socket
     * to cassandra data server
     *
     * @param context is the hadoop reducer context
     */
    protected void setup(Context context) throws IOException {

        log.debug("initializing reducer class for job: " + context.getJobName());
        log.debug("\tinitializing reducer on host: " + InetAddress.getLocalHost().getHostName());

        String assembler = context.getConfiguration().get("assembler.command", "velvet");
        assemblerKeepTopHitOnly = context.getConfiguration().getBoolean("assembler.keeptophit", false);
        assemblerFilterBySizeSpecial = context.getConfiguration().getBoolean("assembler.filterbysizespecial", false);
        assemblerRemoveIdenticalSequences = context.getConfiguration().getBoolean("assembler.removeidenticalsequences", false);

        if ("cap3".equals(assembler)) {

            assemblerCmd = new CapCommand(context.getConfiguration());

        } else if ("velvet".equals(assembler)) {

            assemblerCmd = new VelvetCommand(context.getConfiguration());

        } else {

            throw new IOException("no assembler command provided");

        }


    }

    /**
     * free resource after mapper has finished, ie close socket to cassandra server
     *
     * @param context the reducer context
     */
    protected void cleanup(Context context) {

        if (assemblerCmd != null) assemblerCmd.cleanup();

    }

    /**
     * main reduce step, simply string concatenates all values of a particular key with tab as seperator
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws InterruptedException, IOException {

//            Text reads = new Text();

//            context.getCounter(AssemblyCounters.NUMBER_OF_GROUPS).increment(1);

        log.debug("running reducer class for job: " + context.getJobName());
        log.debug("\trunning reducer on host: " + InetAddress.getLocalHost().getHostName());

        /*
       execute the blast command
        */
        String groupId = key.toString();
        String specialID = null;
        String specialSequence = null;

        Map<String, String> s = null;
        Map<String, String> map = new HashMap<String, String>();

        for (Text r : values) {
            String[] a = r.toString().split("&", 2);

            if (assemblerRemoveIdenticalSequences && map.containsValue(a[1])) {
                continue;
            }

            map.put(a[0], a[1]);
            if (a[0].equals(groupId)) {
                // this sequence is special
                specialID = a[0];
                specialSequence = a[1];
            }
        }

        context.getCounter("reduce.assembly", "NUMBER_OF_READS_IN_GROUP").increment(map.size());
        context.getCounter("reduce", "NUMBER_OF_UNIQUE_GROUPS").increment(1);
        try {
            Map<String, String> tmpmap = assemblerCmd.exec(groupId, map, context);
            ValueComparator bvc =  new ValueComparator(tmpmap);
            s = new TreeMap<String, String>(bvc);
            s.putAll(tmpmap);
        } catch (Exception e) {
            /*
           something bad happened.  update the counter and throw exception
            */
            log.error(e);
//                context.getCounter(AssemblyCounters.NUMBER_OF_ERROR_BLATCOMMANDS).increment(1);
            throw new IOException(e);
        }

        if (s == null) return;

        /*
       assember must have been successful
        */
        //context.getCounter(AssemblyCounters.NUMBER_OF_SUCCESSFUL_BLATCOMMANDS).increment(1);
        //context.getCounter(AssemblyCounters.NUMBER_OF_MATCHED_READS).increment(s.size());

        log.info("assembler retrieved " + s.size() + " results");
        context.getCounter("reduce.assembly", "NUMBER_OF_CONTIGS_ASSEMBLED").increment(s.size());


        for (String k : s.keySet()) {

            if (assemblerFilterBySizeSpecial && specialSequence != null && s.get(k).length() <= specialSequence.length()) continue;

            context.write(new Text(">" + groupId + "-" + k + " numberOfReadsInput=" + map.size() + " numberOfContigs=" + s.size()), new Text("\n" + s.get(k)));

            if (assemblerKeepTopHitOnly) break;

        }

        context.setStatus("Completed");

    }

    class ValueComparator implements Comparator {

        Map base;

        public ValueComparator(Map base) {
            this.base = base;
        }

        public int compare(Object a, Object b) {

            if (((String) base.get(a)).length() < ((String) base.get(b)).length()) {
                return 1;
            } else if (((String) base.get(a)).length() == ((String) base.get(b)).length()) {
                return 0;
            } else {
                return -1;
            }
        }
    }

}
