package gov.jgi.meta.hadoop.map;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.biojava.bio.seq.Sequence;

import java.io.IOException;


/**
 * map class that maps blast output, to expanded set of reads using
 * blat executable.
 */

public class FastaIdentityMapper
        extends Mapper<Text, Sequence, Text, Text> {

    Logger log = Logger.getLogger(this.getClass());

    int mapcount = 0;

    public void map(Text seqid, Sequence s, Context context) throws IOException, InterruptedException {
        String sequence = s.seqString();
        mapcount++;

        String[] seqNameArray = seqid.toString().split("/");

        context.write(new Text(seqid.toString()), new Text(sequence));
    }
    
}
