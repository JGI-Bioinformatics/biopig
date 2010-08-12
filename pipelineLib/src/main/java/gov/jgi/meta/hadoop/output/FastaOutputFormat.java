package gov.jgi.meta.hadoop.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;


public class FastaOutputFormat<K, V> extends TextOutputFormat {

  protected static class FastaRecordWriter<K, V> extends RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";

    private DataOutputStream out;

    public FastaRecordWriter(DataOutputStream out) throws IOException {
      this.out = out;                          
    }

      /**
     * Write the object to the byte stream, handling Text as a special case.
     *
     * @param o
     *          the object to print
     * @throws IOException
     *           if the write throws, we pass it on
     */

    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(utf8));
      }
      out.writeBytes("\n");
    }

    private void writeKey(Object o) throws IOException {
      out.writeBytes(">");
      writeObject(o);
    }

    public synchronized void write(K key, V value) throws IOException {

      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;

      if (nullKey && nullValue) {
        return;
      }

      Object keyObj = key;

      if (nullKey) {
        keyObj = "value";
      }

      writeKey(keyObj);

      if (!nullValue) {
        writeObject(value);
      }

    }

    public synchronized void close(TaskAttemptContext c) throws IOException {
        // even if writeBytes() fails, make sure we close the stream
        out.close();
    }
  }

    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    boolean isCompressed = getCompressOutput(job);
    CompressionCodec codec = null;
    String extension = "";
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass =
        getOutputCompressorClass(job, GzipCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
      extension = codec.getDefaultExtension();
    }
    Path file = getDefaultWorkFile(job, extension);
    FileSystem fs = file.getFileSystem(conf);
    if (!isCompressed) {
      FSDataOutputStream fileOut = fs.create(file, false);
      return new FastaRecordWriter<K, V>(fileOut);
    } else {
      FSDataOutputStream fileOut = fs.create(file, false);
      return new FastaRecordWriter<K, V>(new DataOutputStream
                                        (codec.createOutputStream(fileOut)));
    }
  }
}
