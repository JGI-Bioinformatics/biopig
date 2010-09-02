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
import org.biojavax.bio.seq.RichSequence
import org.biojava.bio.BioException
import org.apache.cassandra.thrift.ColumnPath
import org.apache.cassandra.thrift.ConsistencyLevel
import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.transport.TTransport
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.Mutation
import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.ColumnOrSuperColumn

/**
 * loads sequence data into datastore. command looks like % meta load <table>
 *
 * @param -f file to read and load (or stdin)
 * @param -k keyspace to load into
 * @param -c cassandra host to connect to
 * @param -p port for host
 *
 * @note the cassandra tables must be setup and existent.  they must also have
 * the correct structure.
 */
class loadHashCommand implements command {

    String DEFAULTKEYSPACE = "Keyspace1"
    String DEFAULTTABLE = "Standard1"
    String DEFAULTHOST = "localhost"
    int DEFAULTPORT = 9160
    int DEFAULTKMERSIZE = 20

    List flags = [
    ]

    List params = [
            '-f',   // -f file from which to read sequences
            '-c',   // -c cassandra server to connect to
            '-p',   // -p port number
            '-k',   // -k keyspace
            '-s'    // -s kmer size
    ]

    public static byte[] intToByteArray(long value) {
           byte[] b = new byte[8];
           for (int i = 0; i < 8; i++) {
               int offset = (b.length - 1 - i) * 8;
               b[i] = (byte) ((value >>> offset) & 0xFF);
           }
           return b;
    }

    String name() {
        return "hash"
    }

    List options() {

        /* return list of flags (existential) and parameters  */
        return [
                flags, params
        ];

    }

    String usage() {
        return "hash <table> - loads kmer hashs (of size -s) from file (-f <file> or stdout) into \n\t\tcassandra host (-c) port (-p) using keyspace (-k)";
    }

    /**
     * loads a set of sequence data into cassandra database
     *
     * @param arguments for the load command.  arg[0] is load, arg[1] is the table
     * @param options specifier
     * @return 0 if all went well, not 0 if there was an error
     */
    int execute(List args, Map options) {

        String hostname = options['-c'] ?
            options['-c'] :
            (System.getProperty("meta.defaultHostname") ? System.getProperty("meta.defaultHostname") : DEFAULTHOST)

        println("hostname = " + hostname);
        
        int port = options['-p'] ? Integer.parseInt(options['-p']) : DEFAULTPORT
        int size = options['-s'] ? Integer.parseInt(options['-s']) : DEFAULTKMERSIZE

        Cassandra.Client client = null;

        long timestamp = System.currentTimeMillis();
        String keyspace = options['-k'] ?
            options['-k'] :
            (System.getProperty("meta.defaultKeyspace") ? System.getProperty("meta.defaultKeyspace") : DEFAULTKEYSPACE)

        String table = args[1] ? args[1] : DEFAULTTABLE

        int noconnect = options['-no'] ? 1 : 0

        RichSequenceIterator iter

        def br = options['-f'] ? new BufferedReader(new FileReader(options['-f'])) : new BufferedReader(new InputStreamReader(System.in));

        try {

            iter = (RichSequenceIterator) RichSequence.IOTools.readFastaProtein(br, null);


        } catch (BioException ex) {

            // if error, try to load as sequence data
            try {

                iter = (RichSequenceIterator) RichSequence.IOTools.readFastaDNA(br, null);

            } catch (Exception e) {
                // can't do anything so exit
                println(e);
                return 1;
            }
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

        int count = 0;
        while (iter.hasNext()) {

            RichSequence rr = iter.nextRichSequence();
            String key_user_id
            String segment


            if (rr) {
                if (options['-d']) println(rr.getProperties().URN);
                if (rr.getProperties().URN.find('/')) {
                    def l = rr.getProperties().URN.split('/');
                    key_user_id = l[0]
                    segment = l[1]
                } else {
                    key_user_id = rr.getProperties().URN
                    segment = "0"
                }
            }

            //if (noconnect == 1) { count++; continue;}

            def seq = [:]
            seq["sequence"] = rr.getProperties().stringSequence;



            Map mutation_map = [:]

            /*
            insert data into cassandra
            */
            int seqsize = seq["sequence"].length();

            for (int i = 0; i < seqsize - size -1; i++ ) {
                Mutation kmerinsert = new Mutation();
                count++
                String kmer = seq["sequence"].substring(i, i+size);
                byte[] b = intToByteArray(i);


                if (!mutation_map[kmer]) {
                    mutation_map[kmer] = [:]
                    mutation_map[kmer][table] = []
                }

                if (options['-d']) {
                    println("inserting " + kmer + " into table " + keyspace + "/" + table + " with value " + b);
                    println(seq.toString());

                }

                ColumnOrSuperColumn c = new ColumnOrSuperColumn();
                c.setColumn(new Column(key_user_id.getBytes(), b, timestamp));
                kmerinsert.setColumn_or_supercolumn(c);

                mutation_map[kmer][table].add(kmerinsert);


//                client.insert(keyspace,
//                        kmer,
//                        new ColumnPath(table).setColumn(key_user_id.getBytes()),
//                        b,
//                        timestamp,
//                        ConsistencyLevel.ONE);

                

            }
            //println("mutation_map = " + mutation_map.toString());

            if (noconnect == 1) continue; // useful for timing

            client.batch_mutate(keyspace, mutation_map, ConsistencyLevel.ONE);
        }
        println("inserted " + count + " kmers");
        
        return 0;
    }

}