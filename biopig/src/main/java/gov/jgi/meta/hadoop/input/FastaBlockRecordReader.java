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

package gov.jgi.meta.hadoop.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;


/**
 * Treats keys as offset in file and value as line.
 */
public class FastaBlockRecordReader extends RecordReader<Text, Map<String,String>> {
  private static final Log LOG = LogFactory.getLog(FastaBlockRecordReader.class);

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private FastaBlockLineReader in;
  private int maxLineLength;
  private Text key = null;
  private Map<String,String> value = null;

  FastaBlockRecordReader() {
      LOG.info("creating new fastablockrecordreader");
  }


    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {

      LOG.info("initializing FastaBlockRecordReader");
        
      FileSplit split = (FileSplit) genericSplit;
      Configuration job = context.getConfiguration();
      this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                      Integer.MAX_VALUE);
      start = split.getStart();
      end = start + split.getLength();
      final Path file = split.getPath();
      compressionCodecs = new CompressionCodecFactory(job);
      final CompressionCodec codec = compressionCodecs.getCodec(file);

      // open the file and seek to the start of the split
      FileSystem fs = file.getFileSystem(job);
      FSDataInputStream fileIn = fs.open(split.getPath());
      boolean skipFirstLine = false;
      if (codec != null) {
        in = new FastaBlockLineReader(codec.createInputStream(fileIn), job);
        end = Long.MAX_VALUE;
      } else {
        if (start != 0) {
          skipFirstLine = false;       // don't do this!
          //--start;                      or this
          fileIn.seek(start);
        }
        in = new FastaBlockLineReader(fileIn, job);
      }
      this.pos = start;
    }

  public boolean nextKeyValue() throws IOException {

      LOG.info("retrieving next key/value, pos = " + pos + " : end = " + end);

    if (key == null) {
      key = new Text();
    }
    if (value == null) {
      value = new HashMap<String,String>();
    } else {
        value.clear();
    }

    int newSize = 0;
    while (pos < end) {
      key.set(Long.toString(pos/(end-start)));
      newSize = in.readLine(key, value, maxLineLength,
                            Math.min((int)Math.min(Integer.MAX_VALUE, end-pos),
                                     maxLineLength));

      LOG.info("split value is sizex " + value.size());
      LOG.info("key value = " + key);

      if (newSize == 0) {
        break;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " +
               (pos - newSize));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public Text getCurrentKey() {
    return key;
  }

  @Override
  public Map<String,String> getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}