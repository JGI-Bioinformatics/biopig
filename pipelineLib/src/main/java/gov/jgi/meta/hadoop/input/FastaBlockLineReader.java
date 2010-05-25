/*
 * Copyright (c) 2010, Joint Genome Institute (JGI) United States Department of Energy
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    This product includes software developed by the JGI.
 * 4. Neither the name of the JGI nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY JGI ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL JGI BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package gov.jgi.meta.hadoop.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.biojava.bio.seq.DNATools;
import org.biojava.bio.seq.Sequence;
import org.biojava.bio.symbol.IllegalSymbolException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
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
                        int maxBytesToConsume) throws IOException {

        int totalBytesRead = 0;
        int numRecordsRead = 0;
        Boolean eof = false;
        int startPosn;
        Text recordBlock = new Text();

        LOG.info("starting reading file, buffersize = " + bufferSize);
        LOG.info("max-bytes-to-consume = " + maxBytesToConsume);
        
        /*
        first thing to do is to move forward till you see a start character
         */


        do {
            if (bufferPosn >= bufferLength) {
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
            return totalBytesRead;  // which is 0

        /*
        now bufferPosn should be at the start of a fasta record
         */
        startPosn = bufferPosn-1;  // startPosn guaranteed to be at a ">"

        /*
        find the next record start
         */
        eof = false;
        do {
            if (bufferPosn >= bufferLength) {

//                LOG.info("copy block and refresh from disk. totalbytes read so far = " + totalBytesRead);
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

        } while (buffer[bufferPosn++] != '>' || (totalBytesRead + bufferPosn - startPosn) < maxBytesToConsume);

        LOG.info("last copy to buffer. totalbytes so far = " + totalBytesRead);

        int appendLength = bufferPosn - startPosn;
        recordBlock.append(buffer, startPosn, appendLength);
        totalBytesRead += appendLength;

        LOG.info("finished reading file, parsing data into Map");

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
            while (recordBlock.charAt(j) == CR || recordBlock.charAt(j) == LF) {
                if (j >= recordBlock.getLength()) {
                    LOG.error("underflow! j = " + j + ", length = " + recordBlock.getLength());
                }
                j++;
            }

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
                set.put(k.toString(), s.toString());


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

        LOG.info("done parsing input: " + totalBytesRead + " bytes read, " + numRecordsRead + " records read.");

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

        try {
            FileInputStream fstream = new FileInputStream("/scratch/karan/756.1.948.fas");
            FastaBlockLineReader fblr = new FastaBlockLineReader(fstream);

            Text key = new Text();
            Map<String, String> setofreads = new HashMap<String, String>();
            int length = 500;

            fblr.readLine(key, setofreads, Integer.MAX_VALUE, length);
            System.out.println("setofreads.size = " + setofreads.size());
            for (String s : setofreads.keySet()) {
                System.out.println(s);
            }
            while (setofreads.size() > 0) {
                fblr.readLine(key, setofreads, Integer.MAX_VALUE, length);
                for (String s : setofreads.keySet()) {
                    System.out.println(s);
                }
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}