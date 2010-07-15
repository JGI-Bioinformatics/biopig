package gov.jgi.meta.hadoop.map;

import gov.jgi.meta.exec.BlastCommand;
import gov.jgi.meta.exec.BlatCommand;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class BlastMapperGroupByGene
            extends Mapper<Object, Map<String, String>, Text, Text> {

        static Logger log = Logger.getLogger(BlastMapperGroupByGene.class);
        BlastCommand blastCmd = null;
        BlatCommand blatCmd = null;
        String geneDBFilePath = null;
        Boolean isPaired = true;

        protected void setup(Context context) throws IOException, InterruptedException {

            blastCmd = new BlastCommand(context.getConfiguration());
            blatCmd = new BlatCommand(context.getConfiguration());

            geneDBFilePath = context.getConfiguration().get("blast.genedbfilepath");
            isPaired = context.getConfiguration().getBoolean("blast.readsarepaired", true);

            if (blastCmd == null || blatCmd == null) {
                throw new IOException("unable to create commandline executables");
            }

            /*
            test the existance of the genedbfile
             */
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path filenamePath = new Path(geneDBFilePath);
            if (!fs.exists(filenamePath) || !fs.isFile(filenamePath)) {
                throw new IOException("file (" + geneDBFilePath + ") does not exist or is not a regular file");
            }

        }

        protected void cleanup(Context context) throws IOException {

            if (blastCmd != null) blastCmd.cleanup();
            if (blatCmd != null) blatCmd.cleanup();
            geneDBFilePath = null;

        }

        public void map(Object key, Map<String, String> value, Context context) throws IOException, InterruptedException {

            context.getCounter("map", "NUMBER_OF_READS").increment(value.size());

            /*
            first the blast
             */
            Set<String> s = null;

            try {
                s = blastCmd.exec(value, geneDBFilePath);
            } catch (Exception e) {
                /*
                something bad happened.  update the counter and throw exception
                 */
                log.error(e);
                context.getCounter("blast", "NUMBER_OF_ERROR_BLASTCOMMANDS").increment(1);
                throw new IOException(e);
            }

            /*
            blast executed but did not return sensible values, throw error.
             */
            if (s == null) {
                context.getCounter("blast", "NUMBER_OF_ERROR_BLASTCOMMANDS").increment(1);
                log.error("blast did not execute correctly");
                throw new IOException("blast did not execute properly");
            }

            /*
            blast must have been successful
             */
            context.getCounter("blast", "NUMBER_OF_SUCCESSFUL_BLASTCOMMANDS").increment(1);
            context.getCounter("blast", "NUMBER_OF_MATCHED_READS_AFTER_BLAST").increment(s.size());

            log.debug("blast retrieved " + s.size() + " results");

            for (String k : s) {

                /*
                blast returns the stdout, line by line.  the output is split by tab and
                the first column is the id of the gene, second column is the read id
                 */
                String[] a = k.split("\t");

                /*
                note that we strip out the readid direction.  that is, we don't care if the
                read is a forward read (id/1) or backward (id/2).
                 */

                if (isPaired) {
                    context.write(new Text(a[0]), new Text(a[1].split("/")[0]));
                } else {
                    context.write(new Text(a[0]), new Text(a[1]));
                }
            }

        }
}