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
import java.util.HashMap;
import java.util.Map;

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

            Map<String, String> s = null;
            Map<String, String> map = new HashMap<String, String>();

            for (Text r : values) {
                String[] a = r.toString().split("&", 2);
                map.put(a[0], a[1]);
            }

            try {
                s = assemblerCmd.exec(groupId, map, context);
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

            for (String k : s.keySet()) {

                context.write(new Text(">" + groupId + "-" + k + " numberOfReadsInput=" + map.size() + " numberOfReadsAssembled=" + s.size()), new Text("\n" + s.get(k)));

            }

            context.setStatus("Completed");

        }
    }
