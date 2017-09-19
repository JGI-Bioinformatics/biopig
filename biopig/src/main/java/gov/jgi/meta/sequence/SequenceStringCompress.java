/*
 * Copyright (c) 2017, The Regents of the University of California, through Lawrence Berkeley
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
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;

/**
 * For debug
 * --lanhin
 */
// TODO: Remove these after debug
import org.apache.pig.data.*;
import java.io.IOException;
import java.util.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * This file is modified on the base of SequenceString.java
 * User: lanhin
 * Date: Dec 11, 2016
 * Time: 9:37:22 PM
 */
// TODO: Remove extends after debug
public class SequenceStringCompress extends EvalFunc<DataBag> {

   static HashMap<String, Byte> hash = null;
   static HashMap<Byte, String> reverseHash = null;

    /**
     * Init hash and reverseHash
     * Used in this file, I think.  --lanhin
     */
   public static void init() 
   {
      if (hash == null) {
         hash = new HashMap();
         reverseHash = new HashMap();
         initHash();
      }
   }

    /**
     * To count how many bases there are in a given bytearray
     * The logic has changed  --lanhin
     */
    // TODO: 
    public static int numBases(byte[] seqarray)
    {
	int bitsLengthValid;
	int numberOfFill = (seqarray[0] >>> 6) & 0x3;
	bitsLengthValid = 8 * seqarray.length - 2;
	bitsLengthValid -= bitsLengthValid % 7;
	int last = seqarray[seqarray.length-1] & 0x7f;
	if (last == 0) {
	    bitsLengthValid -= 7;
	}
	return bitsLengthValid / 7 * 3 - numberOfFill;
    }

    /**
     * Return a sub-bytearray of the given one, from start to end
     * The logic has changed  --lanhin
     */
    // TODO: 
    public static byte[] subseq (byte[] seqarray, int start, int end)
    {
        String unpackedSeqSegment = byteArrayToSequence(seqarray);
        return pack(unpackedSeqSegment.substring(start, start+(end-start)));
    }

    /**
     * Check if a base contains in the given sequence in bytearray type
     * --lanhin
     */
    public static boolean contains(String sequence, CharSequence c) throws UnsupportedEncodingException
    {
        return byteArrayToSequence(sequence.getBytes("ISO-8859-1")).contains(c);
    }

    /**
     * Transfer sequence to bytearray
     * --lanhin
     */
   public static byte[] sequenceToByteArray(String sequence)
   {
      init();
      return pack(sequence.toLowerCase());

   }

    /**
     * Transfer bytearray to sequence
     * The logic has changed.  --lanhin
     */
   public static String byteArrayToSequence(byte[] bytes)
   {
      init();

      StringBuffer sb = new StringBuffer();

      int numberOfFill, leftBits, bytesLength = bytes.length;
      byte former, latter, current = 0;

      //In case the input is too short
      if (bytes.length <= 1)
	  return sb.toString();
      
      former = latter = bytes[0];
      numberOfFill = (former >>> 6) & 0x3; //unsigned shift right
      leftBits = 6;

      for (int i = 1; i < bytes.length; i++) {
	  latter = bytes[i];
	  current = (byte)(former << (8 - leftBits));
	  int latter_int = (int)latter & 0xff;
	  current = (byte)(latter_int >>> leftBits | current);
	  current = (byte)((current >>> 1) & 0x7f);
	  if (current == 0)
	      break;
	  
	  sb.append(reverseHash.get(current));
	  leftBits ++;
	  former = latter;
	  if (leftBits == 8) {
	      current = (byte)((former >>> 1) & 0x7f);
	      if (current == 0)
		  break;
	      sb.append(reverseHash.get(current));
	      leftBits = 1;
	  }
      }

      //The last base group will miss only when leftBits = 7
      if ( leftBits == 7 && current != 0){
	  current = (byte)(former << (8 - leftBits));
	  int latter_int = (int)latter & 0xff;
	  current = (byte)(latter_int >>> leftBits | current);
	  current = (byte)((current >>> 1) & 0x7f);

	  //debug
	  System.out.printf("leftBits:%d, current = %d, %s, Fill: %d\n ",leftBits, current, reverseHash.get(current), numberOfFill);    
	  if (current != 0)
	      sb.append(reverseHash.get(current));
      }
      //debug
      System.out.println("sb:"+sb.toString());
      assert(sb.length() > numberOfFill);
      return sb.toString().substring(0, sb.length() - numberOfFill);
   }

    /**
     * Another version
     * Two more parameters
     * This method cannot work correctly with the new compression way.  --lanhin
     */
    /*    public static String byteArrayToSequence(byte[] bytes, int startindex, int length)
    {
       init();

       StringBuffer sb = new StringBuffer();

       for (int i = startindex; i < startindex+length; i++) {
          sb.append(reverseHash.get(bytes[i]));
       }

       return sb.toString();
       }*/

    /**
     * Third version
     * Different input type
     */
    // because Text.bytes.length is not always the right length to use.
    public static String byteArrayToSequence(Text seq)
   {
      byte[] ba = seq.getBytes();

      return byteArrayToSequence(ba);
   }

    /**
     * Init the two hash tables for compress & decompress
     * The logic has changed.  --lanhin
     */
   private static void initHash()
   {
      String[] alphabet = {"a", "t", "g", "c", "n"};

      for (byte i = 0; i < 5; i++) {
         for (byte j = 0; j < 5; j++) {
            for (byte k = 0; k < 5; k++) {
               hash.put(alphabet[i] + alphabet[j] + alphabet[k], (byte) (i * 25 + j * 5 + k + 1));
               reverseHash.put((byte) (i * 25 + j * 5 + k + 1), alphabet[i] + alphabet[j] + alphabet[k]);
            }
         }
      }
   }

    /**
     * The real method to do sequenceToBytearray
     * The logic has changed.  --lanhin
     */
   private static byte[] pack(String sequenceToPack)
   {
      int numberOfBases = sequenceToPack.length();
      int numberOfFill = (3 - numberOfBases % 3)%3;
      int numberOfBytes = (int)Math.ceil(( 2 + Math.ceil((double)numberOfBases / 3) * 7 ) / 8);//Wish I didn't make a mistake here  --lanhin
      int numberOfBaseGroup = (numberOfBases + numberOfFill) / 3;
      
      byte[] bytes = new byte[numberOfBytes];
      int leftBits, position = 0;// Position index in bytes[]

      byte former, latter, current;

      String newSequence = sequenceToPack;
      //Fill by 'n'
      for (int i = 0; i < numberOfFill; i++) {
	  newSequence = newSequence + 'n';
      }

      former = (byte)(((byte)numberOfFill) << 6);//Record this var in the very first two bits.
      leftBits = 6;
      
      try {
	  for (int i = 0; i < numberOfBaseGroup; i++) {

	      StringBuilder subseq = new StringBuilder(newSequence.substring(i*3, i*3+3));
	      for (int si = 0; si < 3; si++) {
		  char sichar = subseq.charAt(si);
		  if (sichar != 'a' && sichar != 't' && sichar != 'g' && sichar != 'c' && sichar != 'n') {
		      subseq.setCharAt(si, 'n');
		  }
	      }
	      current = hash.get(subseq.toString());
	      if (leftBits == 8)
		  former = (byte)(current << 1);
	      else
		  former = (byte)((current >> (7 - leftBits)) | former);
	      latter = (byte)(current << (1+leftBits));
	      if(leftBits <= 7){//A new byte produced
		  bytes[position++] = former;
		  former = latter;
	      }
	      leftBits = (1 + leftBits) % 8;
	      if (leftBits == 0) {
		  leftBits = 8;
	      }
	  }
	  if (leftBits < 8)//Fill the last one byte
	      bytes[position++] = former;
	  
       } catch (Exception e) {
           System.out.println("Pack Error, Please Check."+e);
       }

      // sanity check
      assert (position == numberOfBytes);

      return bytes;
   }

    /**
     * Merge two bytearrays into one
     * --lanhin
     */
    static public byte[] merge(byte[] seq1, byte[] seq2) {

        // reverse the second sequence and merge

        String s1 = byteArrayToSequence(seq1);
        String s2 = byteArrayToSequence(seq2);

        String s3 = s1 + StringUtils.reverse(s2);

        return sequenceToByteArray(s3);
    }

    /**
     * Use this to try to test this unit, wish this can work.
     * --lanhin
     */
    public DataBag exec(Tuple input) throws IOException{
	DataBag output = DefaultBagFactory.getInstance().newDistinctBag();

	byte[] ba;
	String ab;
	Object values = input.get(0);

	try{
	    if (values instanceof DataByteArray) {
		ba = ((DataByteArray) values).get();
		System.out.println("ins of");
		ab = byteArrayToSequence(ba);
		System.out.println("Lengthofbases:"+numBases(ba));
		System.out.println("ab="+ab);
		System.out.println("ba="+ba);
		Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
		t.set(0, ab);
		output.add(t);
	    } else {
		
		System.out.println("[Debug: ] values = "+values);
		ab = values.toString();
		ba = sequenceToByteArray(ab);
		//System.out.println("no ins");

		Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
		t.set(0, new DataByteArray(ba));
		output.add(t);
	    }
	} catch (Exception e) {
	    System.err.println("SequenceStringCompress: failed to process input; error - " + e.getMessage());
	    return(null);
	}

	return output;

    }
}
