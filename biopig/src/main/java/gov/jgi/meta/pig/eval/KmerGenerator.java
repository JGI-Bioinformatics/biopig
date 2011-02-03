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

import gov.jgi.meta.sequence.SequenceString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * This function divides a search query string into wrods and extracts
 * n-grams with up to _ngramSizeLimit length.
 * Example 1: if query = "a real nice query" and _ngramSizeLimit = 2,
 * the query is split into: a, real, nice, query, a real, real nice, nice query
 * Example 2: if record = (u1, h1, pig hadoop) and _ngramSizeLimit = 2,
 * the record is split into: (u1, h1, pig), (u1, h1, hadoop), (u1, h1, pig hadoop)
 */
public class KmerGenerator extends EvalFunc<DataBag> {
   private static final Log LOG = LogFactory.getLog(KmerGenerator.class );

   public DataBag exec(Tuple input) throws IOException
   {
      DataBag output = DefaultBagFactory.getInstance().newDefaultBag();

      if ((input == null) || (input.size() == 0))
      {
         return(null);
      }
      try{
         byte[] ba = ((DataByteArray)input.get(0)).get();
         int kmerSize  = (Integer)input.get(1);
         int seqLength = SequenceString.numBases(ba);

         if (kmerSize > seqLength) { return(null); }

         String kmer;
         for (int i = 0; i <= seqLength - kmerSize; i++)
         {
            kmer = new String(SequenceString.subseq(ba, i, i + kmerSize));
            if ((kmer != null) && !SequenceString.contains(kmer, "n"))
            {
               Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
               t.set(0, kmer);
               output.add(t);
            }
         }
      }
      catch (Exception e) {
         System.err.println("KmerGenerator: failed to process input; error - " + e.getMessage());
         return(null);
      }
      return(output);
   }


   @Override
   public Schema outputSchema(Schema input)
   {
      try {
         Schema.FieldSchema tokenFs = new Schema.FieldSchema("token",
                                                             DataType.BYTEARRAY);
         Schema tupleSchema = new Schema(tokenFs);

         Schema.FieldSchema tupleFs;
         tupleFs = new Schema.FieldSchema("tuple_of_tokens", tupleSchema,
                                          DataType.TUPLE);

         Schema bagSchema = new Schema(tupleFs);
         bagSchema.setTwoLevelAccessRequired(true);
         Schema.FieldSchema bagFs = new Schema.FieldSchema(
            "bag_of_tokenTuples", bagSchema, DataType.BAG);

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
