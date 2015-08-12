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

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import java.io.IOException;
import java.util.Iterator;


/**
 * string.LOWER implements eval function to convert a string to lower case
 * Example:
 *      register pigudfs.jar;
 *      A = load 'mydata' as (name);
 *      B = foreach A generate string.LOWER(name);
 *      dump B;
 */
public class GenerateConsensus extends EvalFunc<Tuple> {

    /**
     * Method invoked on every tuple during foreach evaluation
     * @param input tuple; assumed to be a sequence tuple of the form (id, direction, sequence)
     * @exception java.io.IOException
     */
    public Tuple exec(Tuple input) throws IOException, ExecException {

        DataBag values = (DataBag) input.get(0);

        if(values.size() == 0)
            return null;

        Tuple t = DefaultTupleFactory.getInstance().newTuple(3);
        t.set(0, "mergedsequences");
        t.set(1, 0);
        t.set(2, fastaConsensusSequence(values));

        return t;
    }

    public String fastaConsensusSequence(DataBag values) throws ExecException
    {
       String[] bases = { "a", "t", "g", "c" };
       int[] totals   = { 0, 0, 0, 0 }; // a t g c
       StringBuilder sb = new StringBuilder();

       String[] seqArray = new String[(int) values.size()];


        int ii = 0;
        int maxlength = -1;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext(); ) {
            seqArray[ii] = it.next().get(2).toString();
            if (seqArray[ii].length() > maxlength) maxlength = seqArray[ii].length();
            ii++;
        }
        int length = (int) values.size();

       if (length == 1) { return(seqArray[0]); }
       else
       {
          for (int i = 0; i < maxlength; i++)
          {
             totals[0] = 0;
             totals[1] = 0;
             totals[2] = 0;
             totals[3] = 0;
             for (int j = 0; j < length; j++)
             {
                int[] count = getCounts(seqArray[j], i);
                for (int k = 0; k < 4; k++)
                {
                   totals[k] += count[k];
                }
             }

             int max  = 0;
             int maxk = 0;
             for (int k = 0; k < 4; k++)
             {
                if (totals[k] > max)
                {
                   max  = totals[k];
                   maxk = k;
                }
             }
             sb.append(bases[maxk]);
          }
       }
       return(sb.toString());
    }

    public int[] getCounts(String s, int i)
    {
       int[] totals = { 0, 0, 0, 0 }; // a t g c
       if (i >= s.length()) return totals;

       if (s.charAt(i) == 'a') { totals[0]++; }
       else if (s.charAt(i) == 't') { totals[1]++; }
       else if (s.charAt(i) == 'g') { totals[2]++; }
       else if (s.charAt(i) == 'c') { totals[3]++; }

       return(totals);
    }


}