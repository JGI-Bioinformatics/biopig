/*
 * Copyright (c) 2010, Joint Genome Institute (JGI) United States Department of Energy
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    This product includes software developed by the JGI.
 * 4. Neither the name of the JGI nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY JGI ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL JGI BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package gov.jgi.meta.command

import org.biojavax.bio.seq.RichSequenceIterator
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.SliceRange
import org.apache.cassandra.thrift.KeySlice
import org.apache.cassandra.thrift.ConsistencyLevel
import org.biojavax.bio.seq.RichSequence
import org.biojava.bio.BioException
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TTransport
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.Cassandra
import gov.jgi.meta.MetaUtils



class faDiffCommand implements command {

    List flags = [

    ]

    List params = [

    ]


    String name() {
        return "fadiff"
    }

    List options() {

      /* return list of flags (existential) and parameters  */
      return [
              flags, params
      ];

    }

    String usage() {
        return "fadiff <fileOrDir1> <fileOrDir2>";
    }

    int execute(List<String> args, Map options) {

      Map<String, String> contigs1 = MetaUtils.readSequences(args[1]);
      if (options['-d']) {
        println("read " + args[1] + ": " + contigs1.size() + " sequences")
      }
      Map<String, String> contigs2 = MetaUtils.readSequences(args[2]);
      if (options['-d']) {
        println("read " + args[2] + ": " + contigs2.size() + " sequences")
      }

      int count = 0;
      for (String k : contigs1.keySet()) {
        count++;

        String[] a = k.split("-", 2);
        String key = a[0];

        String s1 = contigs1.get(k);
        String s2 = contigs2.get(key);

        if (s2 == null) {
          println(args[2] + " missing " + key);
        } else {
          println(s2.length() - s1.length());
        }
        contigs2.remove(key);
      }

      for (String k : contigs2.keySet()) {
        String[] a = k.split("-", 2);
        String key = a[0];
        println(args[1] + "missing " + key);
      }

      return 1;
    }


}