package gov.jgi.meta.hadoop.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * simple reducer that just outputs the matches grouped by gene
 */
public class IdentityReducerGroupByKey extends Reducer<Text, Text, Text, Text> {
   public void reduce(Text key, Iterable<Text> values, Context context)
   throws InterruptedException, IOException
   {
      StringBuilder sb = new StringBuilder();

      int i = 0;

      for (Text t : values)
      {
         if (i++ == 0)
         {
            sb.append(t.toString());
         }
         else
         {
            sb.append("\t").append(t.toString());
         }
      }
      context.write(key, new Text(sb.toString()));
   }
}
