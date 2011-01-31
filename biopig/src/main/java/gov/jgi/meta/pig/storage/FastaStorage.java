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
 * A pig loader for fasta files.  The loader reads fasta sequence files and returns tuples of the form
 * <seqid: chararray, direction: int, sequence: chararray>
 **/

public class FastaStorage extends LoadFunc {
   protected RecordReader    in            = null;
   private ArrayList<Object> mProtoTuple   = null;
   private TupleFactory      mTupleFactory = TupleFactory.getInstance();

   /**
    * null constructor
    */
   public FastaStorage()
   {
   }

   /**
    * returns the next sequence from the block
    */
   @Override
   public Tuple getNext() throws IOException
   {

      if (mProtoTuple == null)
      {
         mProtoTuple = new ArrayList<Object>();
      }

      try {
         boolean notDone = in.nextKeyValue();
         if (!notDone)
         {
            return(null);
         }

         /*
           check the id of the sequence to see if its a paired read
          */
         String seqid = ((Text)in.getCurrentKey()).toString();
         String seqkey = null;
         String seqkey2;
         String header = "";
         String direction;
         for (int i = 0; i < seqid.length(); i++) {
            if (seqid.charAt(i) == ' ' || seqid.charAt(i) == '\t') {
               seqkey = seqid.substring(0, i);
               header = seqid.substring(i, seqid.length());
               break;
            }
         }
         if (seqkey == null) seqkey = seqid;
         if (seqkey.indexOf("/") >= 0) {
            String[] a = seqkey.split("/");
            seqkey2 = a[0];
            direction = a[1];
         } else {
            seqkey2 = seqkey;
            direction = "0";
         }
         Text value     = ((Text)in.getCurrentValue());
         mProtoTuple.add(new DataByteArray(seqkey2.getBytes(), 0, seqkey2.length()));                           // add key
         mProtoTuple.add(new DataByteArray(direction.getBytes(), 0, direction.length()));               // add direction
         mProtoTuple.add(new DataByteArray(value.getBytes(), 0, value.getLength()));                       // add sequence
         mProtoTuple.add(new DataByteArray(header.getBytes(), 0, header.length()));                           // add header

         Tuple t = mTupleFactory.newTupleNoCopy(mProtoTuple);
         mProtoTuple = null;
         return(t);
      } catch (InterruptedException e) {
         int    errCode = 6018;
         String errMsg  = "Error while reading input";
         throw new ExecException(errMsg, errCode,
                                 PigException.REMOTE_ENVIRONMENT, e);
      }
   }

   @Override
   public InputFormat getInputFormat()
   {
      return(new FastaInputFormat());
   }

   @Override
   public void prepareToRead(RecordReader reader, PigSplit split)
   {
      in = reader;
   }

   @Override
   public void setLocation(String location, Job job)
   throws IOException
   {
      FileInputFormat.setInputPaths(job, location);
   }
}

// @Test public void testRepeatQueryParams() throws IOException {
// String url = "http://localhost/foo?a=123&a=456nx=y nhttp://localhost/bar?a=761&b=hi";
// QuerystringLoader loader = new QuerystringLoader("a", "b");
// InputStream in = new ByteArrayInputStream(url.getBytes());
// loader.bindTo(null, new BufferedPositionedInputStream(in), 0, url.length());
// Tuple tuple = loader.getNext(); assertEquals("123", (String) tuple.get(0));
// assertNull(tuple.get(1)); tuple = loader.getNext();
// assertEquals(2, tuple.size());
// assertNull(tuple.get(0));
// assertNull(tuple.get(1));
// tuple = loader.getNext();
// assertEquals("761", (String) tuple.get(0));
// assertEquals("hi", (String) tuple.get(1));
// }