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

package gov.jgi.meta.pig.aggregate;

import gov.jgi.meta.MetaUtils;
import gov.jgi.meta.exec.CapCommand;
import gov.jgi.meta.exec.CommandLineProgram;
import gov.jgi.meta.exec.NewblerCommand;
import gov.jgi.meta.sequence.SequenceString;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Pig eval command that given a bag of sequences, assemble them using Cap3 assembler and returns
 * the assembled contigs.
 * <p/>
 * given a bag of sequences, and the number of contigs to return, return either a tuple or a bag
 */
public class ExtendContigWithCap3 extends EvalFunc<Tuple> {
	
	Logger log = Logger.getLogger(ExtendContigWithCap3.class );
	
   /**
    * Method invoked on every tuple during foreach evaluation
    *
    * @param input tuple; assumed to be a sequence tuple of the form (id, direction, sequence)
    * @throws java.io.IOException
    */
   public Tuple exec(Tuple input) throws IOException {
      /*
       * process the inputs ( contigseq, { seq1, seq2, seq3 ... } )
       */
      String contig;

      Object values = input.get(0);
      log.info("Executing ExtendContigWithCap3 values = " + values.getClass().toString());
      if (values instanceof DataByteArray) {
         contig = SequenceString.byteArrayToSequence(((DataByteArray) values).get());
      } else {
         contig = ((String) values);
         
         int s = contig.indexOf('(');
         int e = contig.indexOf(')');
         if (s!=-1 && e!=-1 && s<e){
             contig = contig.substring(s+1,e);
         }
      }

      DataBag bagOfReads = (DataBag) input.get(1);
      String groupId = "contig";
      long numberOfSequences = bagOfReads.size();

      if (numberOfSequences == 0) {
         return (null);
      }

      /*
       * need to load the biopig defaults from the classpath
       */
      Configuration conf = new Configuration();
      MetaUtils.loadConfiguration(conf, "BioPig.xml.magellan", null);
      CommandLineProgram assemblerCmd = new CapCommand(conf);

      /*
       * now process inputs and execute assembler
       */
      Map<String, String> seqMap = new HashMap<String, String>();
      Map<String, String> resultMap;

      Iterator<Tuple> it = bagOfReads.iterator();
      seqMap.put(groupId, contig);
      int readcount = 0;
      while (it.hasNext()) {
         Tuple t = it.next();
         seqMap.put("read" + readcount++, (String) t.get(0));
      }
      
      int error = 0;
      try {
         resultMap = assemblerCmd.exec(groupId, seqMap, null);
      } catch (Exception e) {

         throw new IOException(e);
      }
 
      int maxLength = 0;
      String maxKey = null;
      for (String key : resultMap.keySet()) {
         int l = resultMap.get(key).length();
         if (l > maxLength) {
            maxKey = key;
            maxLength = l;
         }
      }
     
      Tuple t = DefaultTupleFactory.getInstance().newTuple(2);
      
      if (maxLength == 0){
          t.set(0, contig);
          t.set(1, 0);        	 
      }
      else if (maxLength <=contig.length()){
          t.set(0, resultMap.get(maxKey));
          t.set(1, 0);    	  
      }   
      else if (maxLength > contig.length()) {
         t.set(0, resultMap.get(maxKey));
         t.set(1, 1);
      }
      
      return t;

   }
}
