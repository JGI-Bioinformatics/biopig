package test.gov.jgi.meta.pig.eval;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import test.gov.jgi.meta.Util;

import java.io.IOException;
import java.util.Iterator;

public class KNFDistance2Test extends TestCase {
	public KNFDistance2Test(String name) {
		super(name);
	}

	public void setUp() throws Exception {
		super.setUp();
	}

	public void tearDown() throws Exception {
		super.tearDown();
	}

	public void testKmerGenerator() throws IOException {

		PigServer ps = new PigServer(ExecType.LOCAL);
		String script = "A = load 'target/test-classes/threepair.fas' using gov.jgi.meta.pig.storage.FastaStorage as (readid: chararray, d: chararray, seq: bytearray); \n"
				+ "B = foreach A generate readid,gov.jgi.meta.pig.eval.KmerGenerator(seq, 4) as kmers ; \n"
				+

				"BB = foreach B generate readid, flatten(kmers); \n"
				+

				"C = foreach BB  generate readid, gov.jgi.meta.pig.eval.UnpackSequence(kmer) as kmer; \n"
				+

				"D = foreach ( group C by (readid, kmer) ) generate flatten(group), COUNT($1) as cnt; \n"
				+

				"E = foreach (GROUP D by $0) { \n"
				+

				"data = foreach $1 generate $1 as kmer, $2 as cnt; \n"
				+

				" generate $0 as readid, data; \n"
				+

				"}; \n"
				+

				"E2 = foreach E generate *; \n"
				+

				"J = foreach (CROSS E, E2) generate $0 as read1,$2 as read2, ($1,$3) as data; \n"
				+

				"L = FOREACH J GENERATE $0 as read1,$2 as read2, gov.jgi.meta.pig.eval.TNFDistance2(data); \n";

		Util.registerMultiLineQuery(ps, script);
		Iterator<Tuple> it = ps.openIterator("L");

	}

	public static Test suite() {
		return new TestSuite(KNFDistance2Test.class);
	}
}
