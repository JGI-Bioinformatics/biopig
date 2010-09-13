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



class scanCommand implements command {

    String DEFAULTKEYSPACE = "Keyspace1"
    String DEFAULTTABLE = "Standard1"
    String DEFAULTHOST = "localhost"
    String DEFAULTPORT = "9160"

    List flags = [

    ]

    List params = [
            '-c',   // -c cassandra server to connect to
            '-p',   // -p port number
            '-k'    // -k keyspace
    ]


    String name() {
        return "scan"
    }
    
    List options() {

      /* return list of flags (existential) and parameters  */
      return [
              flags, params
      ];

    }

    String usage() {
        return "scan <table> -c <host> -p <port> -k <keyspace>";
    }

    int execute(List args, Map options) {

        BufferedReader br
        RichSequenceIterator iter

        String hostname = options['-c'] ?
            options['-c'] :
            (System.getProperty("meta.defaultHostname") ? System.getProperty("meta.defaultHostname") : DEFAULTHOST)
        
        int port = options['-p'] ? Integer.parseInt(options['-p']) : 9160

       

        Cassandra.Client client = null;

        String keyspace = options['-k'] ?
            options['-k'] :
            (System.getProperty("meta.defaultKeyspace") ? System.getProperty("meta.defaultKeyspace") : DEFAULTKEYSPACE)

        String table = options.args[1]
        if (table == null) {
            println(usage());
            return 1
        }

        /*
         * connect to the cassandra client
         */

        try {
            TTransport tr = new TSocket(hostname, port);
            TProtocol proto = new TBinaryProtocol(tr);
            client = new Cassandra.Client(proto);
            tr.open();
        } catch (Exception e) {
            println("unable to connect to datastore: " + e)
            return 1;
        }

        boolean flag = true;
        String lastkey = "";

        SlicePredicate predicate;
        predicate = new SlicePredicate();
        ColumnParent parent = new ColumnParent(table);
        int count = 0;

        while (flag) {
            predicate.setSlice_range(new SliceRange(new byte[0], new byte[0],false,10))
            List<KeySlice> results = client.get_range_slice(keyspace, parent, predicate, lastkey, "", 10000, ConsistencyLevel.ONE);

            if (lastkey == '') count += results.size();
            else count += results.size()-1;

            if (options['-d']){
                results.each {e ->


                    int colcount = client.get_count(keyspace, e.key, parent, ConsistencyLevel.ONE)
                    print("key: " + e.key + " [" + colcount + " columns]: ");
                    e.columns.each { c ->
                      print( new String(c.column.name) + "/" + byteArrayToInt(c.column.value) + " | ");
                    }
                    if (e.columns.size() < colcount ) {
                       print(" ...\n")
                    } else {
                      print("\n")
                    }
                    //def s = new String(e.columns[0].super_column.name);
                    //def v1 = new String(e.columns[0].super_column.columns[0].value);
                    //def v2 = new String(e.columns[0].super_column.columns[1].value);
                    //println("found: " + s);
                    //println("s1 = " + v1 + "/" + v2);
                }
            }

            if (results.size() < 100) flag = false;
            lastkey = results.last()?.key;

        }

        println("retrieved total of " + count);

        return 1;
    }

  /*
     * Convert the byte array to an int.
     *
     * @param b The byte array
     * @return The integer
     */
    public static int byteArrayToInt(byte[] b) {
        return byteArrayToInt(b, 0);
    }

    /**
     * Convert the byte array to an int starting from the given offset.
     *
     * @param b The byte array
     * @param offset The array offset
     * @return The integer
     */
    public static int byteArrayToInt(byte[] b, int offset) {
        int value = 0;
        for (int i = 0; i < 8; i++) {
            int shift = (8 - 1 - i) * 8;
            value += (b[i + offset] & 0x000000FF) << shift;
        }
        return value;
    }
}