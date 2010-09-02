/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.jgi.meta.pig.storage;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import gov.jgi.meta.hadoop.input.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.biojava.bio.seq.Sequence;

/**
 * A Loader for Hadoop-Fasta files
 **/

public class FastaStorage extends LoadFunc {
    protected RecordReader in = null;
    private byte fieldDel = '\t';
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final int BUFFER_SIZE = 1024;

    public FastaStorage() {
    }

    @Override
    public Tuple getNext() throws IOException {
        if (mProtoTuple == null) {
            mProtoTuple = new ArrayList<Object>();
        }

        try {
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }
            String[] a = ((Text) in.getCurrentKey()).toString().split("/");
            String key = a[0];
            String direction = (a.length > 1 ? a[1] : "");
            String value = ((Sequence) in.getCurrentValue()).seqString();
            mProtoTuple.add(new DataByteArray(key.getBytes(), 0, key.length()));            // add key
            mProtoTuple.add(new DataByteArray(direction.getBytes(), 0, direction.length()));            // add key
            mProtoTuple.add(new DataByteArray(value.getBytes(), 0, value.length()));           // add sequence

            Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
            mProtoTuple = null;
            return t;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }

    }

    @Override
    public InputFormat getInputFormat() {
        return new FastaInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }
}
