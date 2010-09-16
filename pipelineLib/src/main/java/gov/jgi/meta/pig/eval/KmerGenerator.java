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
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
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

    private static final int _ngramSizeLimit = 2;

    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            DataBag output = DefaultBagFactory.getInstance().newDefaultBag();
            String seq = (String)input.get(0);
            //Long d = (Long) input.get(1);
            //String seq = (String) input.get(2);

            Set<String> kmers = generateKmers(seq, 21);
            for (String kmer : kmers) {
                Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
                //t.set(0, seqid);
                t.set(0, kmer);
                output.add(t);
            }
            return output;
        }catch(Exception e){
            System.err.println("KmerGenerator: failed to process input; error - " + e.getMessage());
            return null;
        }
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

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     * This is needed to make sure that both bytearrays and chararrays can be passed as arguments
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

        return funcList;
    }

}
