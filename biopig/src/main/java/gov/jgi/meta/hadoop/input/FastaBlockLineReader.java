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
import org.apache.hadoop.io.Text;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A class that provides a line reader from an input stream.
 */
public class FastaBlockLineReader {
    private static final Log LOG = LogFactory.getLog(FastaBlockLineReader.class);

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private int readBlockSize = 100000;  // default number of reads in each block

    private InputStream in;
    private byte[] buffer;
    // the number of bytes of real data in the buffer
    private int bufferLength = 0;
    // the current position in the buffer
    private int bufferPosn = 0;

    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private static final byte seperator = '>';

    /**
     * Create a line reader that reads from the given stream using the
     * default buffer-size (64k).
     *
     * @param in The input stream
     * @throws java.io.IOException
     */
    public FastaBlockLineReader(InputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Create a line reader that reads from the given stream using the
     * given buffer-size.
     *
     * @param in         The input stream
     * @param bufferSize Size of the read buffer
     * @throws java.io.IOException
     */
    public FastaBlockLineReader(InputStream in, int bufferSize) {
        this.in = in;
        this.bufferSize = bufferSize;
        //     this.bufferSize = DEFAULT_BUFFER_SIZE;
        this.buffer = new byte[this.bufferSize];

    }

    /**
     * Create a line reader that reads from the given stream using the
     * <code>io.file.buffer.size</code> specified in the given
     * <code>Configuration</code>.
     *
     * @param in   input stream
     * @param conf configuration
     * @throws java.io.IOException
     */
    public FastaBlockLineReader(InputStream in, Configuration conf) throws IOException {
        this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
    }

    /**
     * Close the underlying stream.
     *
     * @throws java.io.IOException
     */
    public void close() throws IOException {
        in.close();
    }


    public int readLine(Text key, Map<String, String> set, int maxLineLength,
                        long maxBytesToConsume) throws IOException {

        int totalBytesRead = 0;
        int numRecordsRead = 0;
        Boolean eof = false;
        int startPosn;
        Text recordBlock = new Text();
     

        /*
        first thing to do is to move forward till you see a start character
         */
        startPosn = bufferPosn;
        do {
            if (bufferPosn >= bufferLength) {
                totalBytesRead += bufferPosn - startPosn;
                bufferPosn = 0;
                bufferLength = in.read(buffer);
                if (bufferLength <= 0) {
                    eof = true;
                    break; // EOF
                }
            }
        } while (buffer[bufferPosn++] != '>');

        /*
        if we hit the end of file already, then just return 0 bytes processed
         */
        if (eof)
            return totalBytesRead;

        /*
        now bufferPosn should be at the start of a fasta record
         */
        totalBytesRead += (bufferPosn - 1) - startPosn;
        startPosn = bufferPosn-1;  // startPosn guaranteed to be at a ">"

        /*
        find the next record start
         */
        eof = false;
        do {
            if (bufferPosn >= bufferLength) {

                /*
                copy the current buffer before refreshing the buffer
                 */
                int appendLength = bufferPosn - startPosn;
                recordBlock.append(buffer, startPosn, appendLength);
                totalBytesRead += appendLength;

                startPosn = bufferPosn = 0;
                bufferLength = in.read(buffer);
                if (bufferLength <= 0) {
                    eof = true;
                    break; // EOF
                }
            }

        } while (buffer[bufferPosn++] != '>' || (totalBytesRead + bufferPosn - startPosn) <= maxBytesToConsume);

        if (!eof) {
            bufferPosn--;  // make sure we leave bufferPosn pointing to the next record
            int appendLength = bufferPosn - startPosn;
            recordBlock.append(buffer, startPosn, appendLength);
            totalBytesRead += appendLength;
        }

        /*
        record block now has the byte array we want to process for reads
         */

        Text k = new Text();
        Text s = new Text();
        int i = 1; // skip initial record seperator ">"
        int j = 1;
        do {
            k.clear();
            s.clear();
            /*
            first parse the key
             */
            i = j;
            Boolean junkOnLine = false;
            while (j < recordBlock.getLength()) {
                int c = recordBlock.charAt(j++);
                if (c == CR || c == LF) {
                    break;
                } else if (c == ' ' || c == '\t') {
                    junkOnLine = true;
                    break;
                }
            }
            k.append(recordBlock.getBytes(), i, j - i - 1);

            /*
            in case there is additional metadata on the header line, ignore everything after
            the first word.
             */
            if (junkOnLine) {
                while (j < recordBlock.getLength() && recordBlock.charAt(j) != CR && recordBlock.charAt(j) != LF ) j++;
            }

            //LOG.info ("key = " + k.toString());

            /*
           now skip the newlines
            */
            while (j < recordBlock.getLength() && (recordBlock.charAt(j) == CR || recordBlock.charAt(j) == LF)) j++;

            /*
           now read the sequence
            */
            do {
                i = j;
                while (j < recordBlock.getLength()) {
                    int c = recordBlock.charAt(j++);
                    if (c == CR || c == LF) {
                        break;
                    }
                }
                s.append(recordBlock.getBytes(), i, j - i - 1);
                set.put(k.toString(), s.toString().toLowerCase());

                while (j < recordBlock.getLength() && (recordBlock.charAt(j) == CR || recordBlock.charAt(j) == LF)) j++;

            } while (j < recordBlock.getLength() && recordBlock.charAt(j) != '>');

            numRecordsRead++;

            /*
           now skip characters (newline or carige return most likely) till record start
            */
            while (j < recordBlock.getLength() && recordBlock.charAt(j) != '>') {
                j++;
            }

            j++; // skip the ">"

        } while (j < recordBlock.getLength());

        return totalBytesRead;
    }

    /**
     * Read from the InputStream into the given Text.
     *
     * @param l             the object to store the given line
     * @param maxLineLength the maximum number of bytes to store into str.
     * @return the number of bytes read including the newline
     * @throws java.io.IOException if the underlying stream throws
     */
    public int readLine(Text key, Map<String, String> l, int maxLineLength) throws IOException {
        return readLine(key, l, maxLineLength, Integer.MAX_VALUE);
    }

    /**
     * Read from the InputStream into the given Text.
     *
     * @param l the object to store the given line
     * @return the number of bytes read including the newline
     * @throws java.io.IOException if the underlying stream throws
     */
    public int readLine(Text key, Map<String, String> l) throws IOException {
        return readLine(key, l, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }


    public static void main(String[] args) {

        int num = 1;
        int last = -1;

        do {
        try {                
            FileInputStream fstream = new FileInputStream("/scratch/karan/30mb.fas");
            FastaBlockLineReader fblr = new FastaBlockLineReader(fstream);

            Text key = new Text();
            Map<String, String> setofreads = new HashMap<String, String>();
            Map<String, String> setofreadsTotal = new HashMap<String, String>();
            int length = (int) (Math.random() * 10000);
            length = 3000000;
            System.out.println("lenght = " + length);
            
            int total = 0;

            fblr.readLine(key, setofreads, Integer.MAX_VALUE, length);
//            System.out.println("setofreads.size = " + setofreads.size());
            total += setofreads.size();
            //for (String s : setofreads.keySet()) {
//                System.out.println(s);
//            }
            Runtime r = Runtime.getRuntime();
            while (setofreads.size() > 0) {
                setofreadsTotal.putAll(setofreads);
                setofreads.clear();
                fblr.readLine(key, setofreads, Integer.MAX_VALUE, length);
  //              System.out.println("setofreads.size = " + setofreads.size());
                total += setofreads.size();

                r.gc();
            }
            System.out.println("total = " + total);
            System.out.println("heap size = " + r.totalMemory()/ 1048576);

            if (last != -1) {
               if (last != total) {
                   System.out.println("error!!!, length = " + length + ": last = " + last + " current = " + total);
               }
            }
            last = total;

        } catch (Exception e) {
            System.out.println(e);
        }
        } while (num-- > 0);

      
    }
}