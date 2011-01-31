import java.io.IOException;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This counts the occurrences of words in ColumnFamily Standard1, that has a single column (tha
t we care about)
 * "text" containing a sequence of words.
 *
 * For each word, we output the total number of occurrences across all texts.
 */
public class WordCount extends Configured implements Tool
{
    private static final Logger logger = LoggerFactory.getLogger(WordCount.class);

    static final String KEYSPACE = "jgi";
    static final String COLUMN_FAMILY = "reads";
    private static final String CONF_COLUMN_NAME = "columnname";
    private static final String OUTPUT_PATH_PREFIX = "/tmp/seq_count3";
    static final int RING_DELAY = 3000; // this is enough for testing a single server node; mayneed more for a real cluster

    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(0);
    }

    public static class TokenizerMapper extends Mapper<String, SortedMap<byte[], IColumn>, Text,
 IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String columnName;

        public void map(String key, SortedMap<byte[], IColumn> columns, Context context) throws IOException, InterruptedException
        {
            logger.info("inside TokenizerMapper:Map() function: key = " + key + "/" + columnName);
            IColumn column = columns.get(columnName.getBytes());
            if (column == null)
                return;

            String value = nw String(column.getSubColumn("1".getBytes()).value()).substring(0,20) +
                new String(column.getSubColumn("2".getBytes()).value()).substring(0,20);

            word.set(value);
            context.write(word, one);

        }

        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException
        {

        }

    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;

            logger.info("inside IntSumReducer... karan");

            for (IntWritable val : values)
            {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();

        logger.info("creating " + 1 + "jobs - karan");

        for (int i = 0; i < 1; i++)
        {
            logger.info("Job " + i);

            String columnName = "sequence";

            logger.info("looking at column: " + columnName);

            conf.set(CONF_COLUMN_NAME, columnName);
            Job job = new Job(conf, "wordcount");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setInputFormatClass(ColumnFamilyInputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_PREFIX + i));

            ConfigHelper.setColumnFamily(job.getConfiguration(), KEYSPACE, COLUMN_FAMILY);
//            ConfigHelper.setInputSplitSize(job.getConfiguration(), 10);
            SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(columnName.getBytes()));
            ConfigHelper.setSlicePredicate(job.getConfiguration(), predicate);

            job.waitForCompletion(true);
        }
        return 0;
    }
}
