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
import gov.jgi.meta.exec.BlastCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.*;


/**
 * Pig eval command that given a bag of sequences, assemble them using Cap3 assembler and returns
 * the assembled contigs.
 *
 * given a bag of sequences, and the number of contigs to return, return either a tuple or a bag
 */
public class kmerMatch extends EvalFunc<DataBag> {
   /**
    * Method invoked on every tuple during foreach evaluation
    * @param input tuple; assumed to be a sequence tuple of the form (id, direction, sequence)
    * @exception java.io.IOException
    */

   
   public DataBag exec(Tuple input) throws IOException
   {
      DataBag output = DefaultBagFactory.getInstance().newDefaultBag();

      /*
       * process the inputs (bagOfSequences, optionalNumberOfContigsToReturn, optionalGroupId)
       */
      DataBag values     = (DataBag)input.get(0);
      String databaseFilename = (String) input.get(1);
      int kmerSize = (Integer) input.get(2);

      long numberOfSequences = values.size();

      if (numberOfSequences == 0)
      {
         return(null);
      }

      /*
       * need to load the biopig defaults from the classpath
       */
      Configuration conf = new Configuration();
      MetaUtils.loadConfiguration(conf, "BioPig.xml", null);

      /*
       * now process inputs and execute blast
       */
      Map<String, Boolean> seqMap = new HashMap<String, Boolean>();
      Set<String> s = new HashSet<String>();

      Iterator<Tuple> it = values.iterator();
      Set<String> kmers = new HashSet<String>();
      Map<String, String> dbsequences = MetaUtils.readSequences(databaseFilename);
      int ii = 0;

      while (it.hasNext())
      {
         System.out.println("calculating... " + ii++);

         Tuple t = it.next();
         String seq = (String) t.get(2);
         String seqid = (String)t.get(0) + "/" + (Integer)t.get(1);

         kmers.addAll(generateKmers(seq, kmerSize));
      }

         System.out.println("size = " + dbsequences.size());

         for (String d : dbsequences.keySet()) {

            int window = kmerSize;
            String seq2 = dbsequences.get(d);
            int seqLength = seq2.length();

            for (int i = 0; i < seqLength-window-1; i++) {
               String kmer = seq2.substring(i, i+window);
               if (kmers.contains(kmer)) {
                  s.add(d);
                  break;
               }
            }

         }

      Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
      DataBag oo = DefaultBagFactory.getInstance().newDefaultBag();

      for (String k : s) {

         Tuple tt = DefaultTupleFactory.getInstance().newTuple(1);
         tt.set(0, k);
         oo.add(tt);

      }
      t.set(0, oo);
      output.add(t);

      return(output);
   }

   Set<String> generateKmers(String s, int window)
   {
      Set<String> set= new HashSet<String>();

      int seqLength = s.length();

       if (window > seqLength) return set;

       for (int i = 0; i < seqLength-window-1; i++) {
           set.add(s.substring(i, i+window));
       }

       return set;
   }

}