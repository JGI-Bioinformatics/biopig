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

package gov.jgi.meta.sequence;

import org.apache.hadoop.io.Text;

import java.util.HashMap;

/**
 * Created by IntelliJ IDEA.
 * User: kbhatia
 * Date: Dec 3, 2010
 * Time: 9:37:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class SequenceString {

   static HashMap<String, Byte> hash = null;
   static HashMap<Byte, String> reverseHash = null;

   public static void init() 
   {
      if (hash == null) {
         hash = new HashMap();
         reverseHash = new HashMap();
         initHash();
      }
   }

    public static int numBases(byte[] seqarray)
    {
        int lastByte = seqarray.length - 1;
        int lastByteNumBases = reverseHash.get(seqarray[lastByte]).length();
        return (3 * lastByte + lastByteNumBases);
    }

    public static byte[] subseq (byte[] seqarray, int start, int end)
    {
        int startindex = start/3;
        int overflow = ( ((end%3) == 0) ? 0 : 1);
        int endindex = end/3 + overflow;

        String unpackedSeqSegment = byteArrayToSequence(seqarray, startindex, endindex-startindex);
        return pack(unpackedSeqSegment.substring(start%3, start%3+(end-start)));

    }

    public static boolean contains(String sequence, CharSequence c)
    {
        return byteArrayToSequence(sequence.getBytes()).contains(c);
    }

   public static byte[] sequenceToByteArray(String sequence)
   {
      init();
      return pack(sequence);

   }
   public static String byteArrayToSequence(byte[] bytes)
   {
      init();

      StringBuffer sb = new StringBuffer();

      for (int i = 0; i < bytes.length; i++) {
         sb.append(reverseHash.get(bytes[i]));
      }

      return sb.toString();
   }

    public static String byteArrayToSequence(byte[] bytes, int startindex, int length)
    {
       init();

       StringBuffer sb = new StringBuffer();

       for (int i = startindex; i < startindex+length; i++) {
          sb.append(reverseHash.get(bytes[i]));
       }

       return sb.toString();
    }

    // because Text.bytes.length is not always the right length to use.
public static String byteArrayToSequence(Text seq)
   {
      init();

      StringBuffer sb = new StringBuffer();
      byte[] ba = seq.getBytes();

      for (int i = 0; i < seq.getLength() ; i++) {
         sb.append(reverseHash.get(ba[i]));
      }

      return sb.toString();
   }


   private static void initHash()
   {
      String[] alphabet = {"a", "t", "g", "c", "n"};

      for (byte i = 0; i < 5; i++) {
         hash.put(alphabet[i], (byte) (32 + 128 + i ));
         reverseHash.put((byte) (32 + 128 + i ), alphabet[i]);
      }

      for (byte i = 0; i < 5; i++) {
         for (byte j = 0; j < 5; j++) {
            hash.put(alphabet[i] + alphabet[j], (byte) (32 + 192 + i * 5 + j ));
            reverseHash.put((byte) (32 + 192 + i * 5 + j ), alphabet[i] + alphabet[j]);
         }
      }

      for (byte i = 0; i < 5; i++) {
         for (byte j = 0; j < 5; j++) {
            for (byte k = 0; k < 5; k++) {
               hash.put(alphabet[i] + alphabet[j] + alphabet[k], (byte) (32 + i * 25 + j * 5 + k));
               reverseHash.put((byte) (32 + i * 25 + j * 5 + k), alphabet[i] + alphabet[j] + alphabet[k]);
            }
         }
      }
   }

   private static byte[] pack(String sequenceToPack)
   {
      int numberOfBases = sequenceToPack.length();
      int numberOfFullBytes = numberOfBases/3;
      int overflow = (numberOfBases % 3 == 0 ? 0 : 1);
      int numberOfBytes = numberOfFullBytes + overflow;

      byte[] bytes = new byte[numberOfBytes];

      int i = 0;

       try {
      for (i = 0; i < numberOfFullBytes; i++) {

         String subseq = sequenceToPack.substring(i*3, i*3+3);
         if (subseq.matches(".*[^atgcn].*")) {
             subseq = subseq.replaceAll("[^atgcn]", "n");
         }
         bytes[i] = hash.get(subseq);

      }
       } catch (Exception e) {
           System.out.println("i = " + i);
       }

      if (overflow > 0) {
         String subseq = sequenceToPack.substring(i*3, i*3 + numberOfBases % 3);
         if (subseq.matches("[^atgcn]")) {
             subseq = subseq.replaceAll("[^atgcn]", "n");
         }
         bytes[i++] = hash.get(subseq);
      }

      // sanity check
      assert (i == numberOfBytes);

      return bytes;
   }
}
