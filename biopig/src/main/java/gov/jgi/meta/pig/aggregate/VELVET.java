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
import gov.jgi.meta.exec.VelvetCommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Pig eval command that given a bag of sequences, assemble them using Cap3 assembler and returns
 * the assembled contigs.
 *
 * given a bag of sequences, and the number of contigs to return, return either a tuple or a bag
 */
public class VELVET extends EvalFunc<Tuple> {
   /**
    * Method invoked on every tuple during foreach evaluation
    * @param input tuple; assumed to be a sequence tuple of the form (id, direction, sequence)
    * @exception java.io.IOException
    */
   public Tuple exec(Tuple input) throws IOException
   {
      /*
       * process the inputs (bagOfSequences, optionalNumberOfContigsToReturn, optionalGroupId)
       */
      DataBag values     = (DataBag)input.get(0);
      Long    numContigs = new Long(1); // by default only return a single contig

      if (input.size() > 1)
      {
         numContigs = (Long)input.get(1);
      }
      String groupId = "";
      if (input.size() > 2)
      {
         groupId = (String)input.get(2);
      }

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
      CommandLineProgram assemblerCmd = new VelvetCommand(conf);

      /*
       * now process inputs and execute assembler
       */
      Map<String, String> seqMap = new HashMap<String, String>();
      Map<String, String> resultMap;

      Iterator<Tuple> it = values.iterator();
      while (it.hasNext())
      {
         Tuple t = it.next();
         seqMap.put((String)t.get(0) + "/" + (Integer)t.get(1), (String)t.get(2));
      }
      try {
         resultMap = assemblerCmd.exec(groupId, seqMap, null);
      } catch (InterruptedException e) {
         throw new IOException(e);
      }

      int    maxLength = 0;
      String maxKey    = null;
      for (String key : resultMap.keySet())
      {
         int l;
         if ((l = resultMap.get(key).length()) > maxLength)
         {
            maxKey    = key;
            maxLength = l;
         }
      }

      Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
      t.set(0, resultMap.get(maxKey));

      return(t);
   }

   boolean arePairedSequences(Tuple s1, Tuple s2)
   {
      return(true);
   }
}