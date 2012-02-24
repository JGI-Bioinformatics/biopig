/*
 * Copyright (c) 2010, The Regents of the University of California, through Lawrence Berkeley
 * National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy).
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * (1) Redistributions of source code must retain the above copyright notice, this list of conditions and the
 * following disclaimer.
 *
 * (2) Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 * and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * (3) Neither the name of the University of California, Lawrence Berkeley National Laboratory, U.S. Dept.
 * of Energy, nor the names of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You are under no obligation whatsoever to provide any bug fixes, patches, or upgrades to the
 * features, functionality or performance of the source code ("Enhancements") to anyone; however,
 * if you choose to make your Enhancements available either publicly, or directly to Lawrence Berkeley
 * National Laboratory, without imposing a separate written license agreement for such Enhancements,
 * then you hereby grant the following license: a  non-exclusive, royalty-free perpetual license to install,
 * use, modify, prepare derivative works, incorporate into other computer software, distribute, and
 * sublicense such enhancements or derivative works thereof, in binary and source code form.
 */

package gov.jgi.meta.pig.storage;

import gov.jgi.meta.hadoop.input.FastaBlockInputFormat;
import gov.jgi.meta.hadoop.input.FastaBlockRecordReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * A data loader for fasta files. Loads sequences in blocks defined by the hdfs blocksize. Typically, each mapper will load a single block of sequences, as opposed to loading each
 * sequence individually. The advantage of loading them in blocks is where you want to operate on blocks at a time, eg. running blast against a dataset. In this case, you don't
 * want to run blast on each sequence, rather, you want to load a block of sequences (or a bag in pig parlance), and run blast against them all. The blocks are guaranteed to be
 * co-located within the a single map.
 * 
 * returns a bag in the form {offset: int, sequences: { seq1, seq2, seq3 ... } } where each sequence is of the form {id: chararray, direction: int, sequence: chararray}
 * 
 **/

public class FastaBlockStorage extends LoadFunc {
	private static final Log LOG = LogFactory.getLog(FastaBlockStorage.class);

	protected RecordReader in = null;
	private ArrayList<Object> mProtoTuple = null;
	private TupleFactory mTupleFactory = TupleFactory.getInstance();

	/**
	 * Null constructor
	 */
	public FastaBlockStorage() {
		LOG.info("initializing FastaBlockStorage");
	}

	/**
	 * read input and return next tuple.
	 * 
	 * @return Tuple of the form <offset: int, sequences:bag>
	 * @throws IOException
	 *             if any error occurs
	 */
	@Override
	public Tuple getNext() throws IOException {
		if (mProtoTuple == null) {
			mProtoTuple = new ArrayList<Object>();
		}

		try {
			LOG.info("loading next key/value");
			boolean notDone = ((FastaBlockRecordReader) in).nextKeyValue();
			LOG.info("notDone = " + notDone);

			if (!notDone) {
				return (null);
			}

			LOG.info("retrieving key/value from fastablockstorage");
			String key = ((Text) in.getCurrentKey()).toString();
			LOG.info("key = " + key);
			Map<String, String> seqMap = (Map<String, String>) in.getCurrentValue();
			LOG.info("value map size = " + seqMap.size());
			DataBag output = DefaultBagFactory.getInstance().newDefaultBag();

			for (String seqid : seqMap.keySet()) {
				Tuple t = DefaultTupleFactory.getInstance().newTuple(3);

				/*
				 * check the id of the sequence to see if its a paired read
				 */
				String seqkey;
				int direction;
				if (seqid.indexOf("/") >= 0) {
					String[] a = seqid.split("/");
					seqkey = a[0];
					direction = Integer.parseInt(a[1]);
				} else {
					seqkey = seqid;
					direction = 0;
				}
				t.set(0, seqkey);
				t.set(1, direction);
				t.set(2, seqMap.get(seqid));

				output.add(t);
			}
			mProtoTuple.add(new DataByteArray(key.getBytes(), 0, key.length()));
			mProtoTuple.add(output);

			Tuple t = mTupleFactory.newTupleNoCopy(mProtoTuple);
			mProtoTuple = null;

			LOG.info("loaded " + output.size() + " tuples, t = " + t.getMemorySize());
			return (t);
		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		}
	}

	@Override
	public InputFormat getInputFormat() {
		FastaBlockInputFormat in = new FastaBlockInputFormat();

		return (in);
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
		in = reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}
}
