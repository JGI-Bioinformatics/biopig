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
package gov.jgi.meta.pig.eval;

import java.io.IOException;
import java.util.*;

import gov.jgi.meta.sequence.SequenceStringCompress;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Kmer generator with better compression ratio.
 * --lanhin
 */
public class KmerGeneratorStepCoprs extends EvalFunc<DataBag> {

    private static final Logger log = LoggerFactory.getLogger(KmerGeneratorWithStep.class );
    
    static HashMap<Character, Character> hash = null;

    public static void init(){
	if (hash == null){
	    hash = new HashMap();

	    hash.put ('a', 't');
	    hash.put ('c', 'g');
	    hash.put ('g', 'c');
	    hash.put ('t', 'a');
	    hash.put ('n', 'n');
	}
    }

    /**
     * input: (string sequence, int k, int step(1), bool revcomp(true) )
     *
     */
   public DataBag exec(Tuple input) throws IOException
   {
       DataBag output = DefaultBagFactory.getInstance().newDistinctBag();
       int start, num, end;
       boolean RevCom = true;

       init();

      if ((input == null) || (input.size() == 0))
      {
         return(null);
      }

      try{
          String ba, baRc;//baRc for reverse complement
          Object values = input.get(0);
          if (values instanceof DataByteArray) {
              // i know this is somewhat inefficient, but quick and dirty
              ba = SequenceStringCompress.byteArrayToSequence(((DataByteArray)values).get());
          } else {
	      ba = ((String)values).toLowerCase();
          }
 
         int kmerSize  = (Integer)input.get(1);
         int seqLength = ba.length();
	 int Step = 1; //Step for next kmer.

	 //log.info("OrigRead=" + OrigRead);
	 //Get the reverse complement of ba
	 char[] readComplement = new char[seqLength + 1];
	 for (int i=0; i < seqLength; i++){
	     readComplement[i] = hash.get(ba.charAt(seqLength-i-1));
	 }
	 //String RCRead = new String(readComplement);
	 baRc = String.valueOf(readComplement);
	 //log.info("RCRead=" + RCRead);
	 
         // defaults
         start = 0;
         end = seqLength - kmerSize;
         num = end - start + 1;

          if (input.size() == 2) {
              // this is the regular kmergenerator function

          } else if (input.size() == 3) {
              // user added a step
              Step = (Integer) (input.get(2));
              if (Step <= 0) {
		  log.warn("Invalid step value: "+Step+", use step=1 instead.");
		  Step = 1;
              }
          } else if (input.size() == 4){
	      Step = (Integer) (input.get(2));
              if (Step <= 0) {
		  log.warn("Invalid step value: "+Step+", use step=1 instead.");
		  Step = 1;
              }
	      //Check if need reverse complement kmer
	      RevCom = (boolean) (input.get(3));
	  }
	  
         if (kmerSize > seqLength) { return(null); }

         String kmer, kmerRC;
         for (int i = start; i <= end; i += Step)
         {
	    String kmerba = ba.substring(i, i + kmerSize);
	    String kmerbaRc = baRc.substring(i, i + kmerSize);
	    
            if ((kmerba != null) && !kmerba.contains("n"))
            {
		kmer = new String(SequenceStringCompress.sequenceToByteArray(kmerba), "ISO-8859-1");
		Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
		t.set(0, new DataByteArray(kmer.getBytes("ISO-8859-1")));
		output.add(t);
            }
	    if (RevCom && (kmerbaRc != null) && !kmerbaRc.contains("n")){
		kmerRC = new String(SequenceStringCompress.sequenceToByteArray(kmerbaRc), "ISO-8859-1");
		Tuple t1 = DefaultTupleFactory.getInstance().newTuple(1);
		t1.set(0, new DataByteArray(kmerRC.getBytes("ISO-8859-1")));
		output.add(t1);
	    }

         }
      }
      catch (Exception e) {
         System.err.println("KmerGeneratorWithRevComp: failed to process input; error - " + e.getMessage());
         return(null);
      }
      return(output);
   }
   @Override
   public Schema outputSchema(Schema input)
   {
      try {
         Schema.FieldSchema tokenFs = new Schema.FieldSchema("kmer",
                                                             DataType.BYTEARRAY);
         Schema tupleSchema = new Schema(tokenFs);

         Schema.FieldSchema tupleFs;
         tupleFs = new Schema.FieldSchema("tuple_of_kmer", tupleSchema,
                                          DataType.TUPLE);

         Schema bagSchema = new Schema(tupleFs);
         bagSchema.setTwoLevelAccessRequired(true);
         Schema.FieldSchema bagFs = new Schema.FieldSchema(
            "kmers", bagSchema, DataType.BAG);

         return(new Schema(bagFs));
      }
      catch (FrontendException e) {
         // throwing RTE because
         //above schema creation is not expected to throw an exception
         // and also because superclass does not throw exception
         throw new RuntimeException("Unable to compute TOKENIZE schema.");
      }
   }


}
