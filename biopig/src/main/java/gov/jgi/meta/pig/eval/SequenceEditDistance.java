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
package gov.jgi.meta.pig.eval;

import gov.jgi.meta.MetaUtils;
import gov.jgi.meta.sequence.SequenceString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Set;

/**
 */
public class SequenceEditDistance extends EvalFunc<DataBag> {
    private static final Log LOG = LogFactory.getLog(SequenceEditDistance.class);

    public DataBag exec(Tuple input) throws IOException {

       DataBag output = DefaultBagFactory.getInstance().newDefaultBag();

        if (input == null || input.size() == 0)
            return null;
        try{
           String seq = ((String) input.get(0));
           //byte[] ba  = ((DataByteArray) input.get(0)).get();
           int editDistance = (Integer) input.get(1);
           //int seqLength = SequenceString.numBases(ba);
           //String seq = SequenceString.byteArrayToSequence(ba);
           
           Set<String> neighbors = MetaUtils.generateAllNeighbors(seq, editDistance);
           for (String n : neighbors) {
                Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
                t.set(0, n);
                output.add(t);
              }
        }catch(Exception e){
            System.err.println("sequenceeditdistance: failed to process input; error - " + e.getMessage());
            return null;
        }
       return output;
   }

    @Override
    public Schema outputSchema(Schema input) {

        try {
            Schema.FieldSchema tokenFs = new Schema.FieldSchema("token",
                    DataType.CHARARRAY);
            Schema tupleSchema = new Schema(tokenFs);

            Schema.FieldSchema tupleFs;
            tupleFs = new Schema.FieldSchema("tuple_of_tokens", tupleSchema,
                    DataType.TUPLE);

            Schema bagSchema = new Schema(tupleFs);
            bagSchema.setTwoLevelAccessRequired(true);
            Schema.FieldSchema bagFs = new Schema.FieldSchema(
                        "bag_of_tokenTuples",bagSchema, DataType.BAG);

            return new Schema(bagFs);

        } catch (FrontendException e) {
            // throwing RTE because
            //above schema creation is not expected to throw an exception
            // and also because superclass does not throw exception
            throw new RuntimeException("Unable to compute TOKENIZE schema.");
        }
    }

}