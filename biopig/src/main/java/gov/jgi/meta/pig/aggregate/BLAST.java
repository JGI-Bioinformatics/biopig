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
import gov.jgi.meta.exec.CapCommand;
import gov.jgi.meta.exec.CommandLineProgram;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.*;


/**
 * Pig eval command that given a bag of sequences, assemble them using Cap3 assembler and returns
 * the assembled contigs.
 * <p/>
 * given a bag of sequences, and the number of contigs to return, return either a tuple or a bag
 */
public class BLAST extends EvalFunc<DataBag> implements Accumulator<DataBag> {
   /**
    * Method invoked on every tuple during foreach evaluation
    *
    * @param input tuple; assumed to be a sequence tuple of the form (id, direction, sequence)
    * @exception java.io.IOException
    */
   DataBag output = null;
   Configuration conf = null;
   String databaseFilename = null;
   Map<String, String> seqMap = null;

   public DataBag exec(Tuple input) throws IOException {
      accumulate(input);
      return getValue();
   }

   public void accumulate(Tuple input) throws IOException {
      if (output == null) {
         output = DefaultBagFactory.getInstance().newDefaultBag();
      }

      if (conf == null) {
         conf = new Configuration();
         MetaUtils.loadConfiguration(conf, "BioPig.xml", null);
      }

      if (seqMap == null) {
         seqMap = new HashMap<String, String>();
      }
      /*
       * process the inputs (bagOfSequences, optionalNumberOfContigsToReturn, optionalGroupId)
       */
      DataBag values = (DataBag) input.get(0);
      databaseFilename = (String) input.get(1);
      
     // long numberOfSequences = values.size();

      //if (numberOfSequences == 0) {
//         return;
//      }

      Iterator<Tuple> it = values.iterator();
      while (it.hasNext()) {
         Tuple t = it.next();
         seqMap.put((String) t.get(0) + "/" + (Integer) t.get(1), (String) t.get(2));
      }


   }

   @Override
   public void cleanup() {
      output = null;
      conf = null;
      databaseFilename = null;
      seqMap = null;
   }

   @Override
   public DataBag getValue() {

      /*
      * need to load the biopig defaults from the classpath
      */


      /*
       * now process inputs and execute blast
       */

      Set<String> s;
      Map<String, Set<String>> resultMap = new HashMap<String, Set<String>>();


      try {
         BlastCommand blastCmd = new BlastCommand(conf);
         s = blastCmd.exec(seqMap, databaseFilename);

         for (String k : s) {
            /*
            * blast returns the stdout, line by line.  the output is split by tab and
            * the first column is the id of the gene, second column is the read id
            */
            String[] a = k.split("\t");

            if (resultMap.containsKey(a[0])) {
               resultMap.get(a[0]).add(a[1]);
            } else {
               resultMap.put(a[0], new HashSet<String>());
               resultMap.get(a[0]).add(a[1]);
            }
         }

         for (String k : resultMap.keySet()) {
            Tuple t = DefaultTupleFactory.getInstance().newTuple(2);

            t.set(0, k);

            DataBag oo = DefaultBagFactory.getInstance().newDefaultBag();
            for (String kk : resultMap.get(k)) {
               Tuple tt = DefaultTupleFactory.getInstance().newTuple(3);

               String[] a = kk.split("/");
               tt.set(0, a[0]);
               tt.set(1, a[1]);
               tt.set(2, seqMap.get(kk));

               oo.add(tt);
            }

            t.set(1, oo);
            output.add(t);
         }
      } catch (Exception e) {
      }

      return (output);
   }

   @Override
   public Schema outputSchema(Schema input)
   {
      try {
         Schema tupleSchema = new Schema();
         Schema seqSchema = new Schema();

         // first define the sequence schema id, d, sequence
         seqSchema.add(new Schema.FieldSchema("id", DataType.CHARARRAY));
         seqSchema.add(new Schema.FieldSchema("d", DataType.INTEGER));
         seqSchema.add(new Schema.FieldSchema("seq", DataType.CHARARRAY));

         // now define the tuple
         tupleSchema.add(new Schema.FieldSchema("geneid", DataType.CHARARRAY));
         tupleSchema.add(new Schema.FieldSchema("setofsequences", seqSchema, DataType.BAG));

         Schema.FieldSchema bagFs = new Schema.FieldSchema(
            "blastmatches", tupleSchema, DataType.BAG);

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
