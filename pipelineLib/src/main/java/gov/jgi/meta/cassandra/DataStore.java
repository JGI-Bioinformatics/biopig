package gov.jgi.meta.cassandra;

import net.sf.json.JSONObject;
import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: kbhatia
 * Date: Apr 26, 2010
 * Time: 11:06:07 AM
 * To change this template use File | Settings | File Templates.
 */
public class DataStore {



    Logger log = Logger.getLogger(DataStore.class);


    /*
    cassandra configuration parameters
     */
    TTransport tr = null;
    TProtocol proto = null;
    Cassandra.Client client = null;
    String cassandraTable = null;
    public String cassandraHost = null;
    int cassandraPort = 0;
    String keyspace = null;


    /*
    batched operations
     */
    HashMap<String, HashMap<String, LinkedList<Mutation>>> mutation_map = null;
    int currentSize = 0;


    /**
     * intializes the connection to datastore
     *
     * @return 1 if successful, 0 if error.
     */

    public void initialize(Configuration c) throws IOException {


        if (tr != null) cleanup();

        cassandraHost = c.get("cassandrahost");
        cassandraPort = c.getInt("cassandraport", 9160);
        keyspace = c.get("keyspace");
        JSONObject hostmapping = null;
        String hostname = InetAddress.getLocalHost().getHostName();

        if (c.get("datahostmapping") != null)
            hostmapping = JSONObject.fromObject(c.get("datahostmapping"));

        try {


            /*
           need to determine the datahost to talk with.  the host specified is only the seed, there may be
           others and different hosts should spread out connections.  If user has specified a host mapping,
           than use that, otherwise, query the seed to determine the set of hosts, and pick one at random
           (but try to ensure that the same host goes to the same server every time)
            */

            String useHost = null;
            if (hostmapping != null && hostmapping.get(hostname) != null) {

                useHost = hostmapping.getString(hostname);
                log.info("my mapping = " + useHost);

            } else {

                tr = new TSocket(cassandraHost, cassandraPort);
                proto = new TBinaryProtocol(tr);
                client = new Cassandra.Client(proto);
                tr.open();

                String jsonhostList = client.get_string_property("token map");  // this is a json map
                JSONObject jsonObject = JSONObject.fromObject(jsonhostList);
                Set s = jsonObject.keySet();
                log.info("\tnumber of hosts: " + s.size());
                int i = hashHost(hostname, s.size());

                useHost = jsonObject.values().toArray()[i].toString();
                log.info("\tpicking host: " + useHost);

            }

            if (useHost != null) {

                if (tr != null) tr.close();

                cassandraHost = useHost;
                log.info("\tconnecting to " + cassandraHost + "/" + cassandraPort);
                tr = new TSocket(cassandraHost, cassandraPort);
                proto = new TBinaryProtocol(tr);
                client = new Cassandra.Client(proto);
                tr.open();
            }

            mutation_map = null;
            currentSize = 0;

        } catch (Exception e) {
            log.fatal("ERROR: " + e);
            throw new IOException("unable to connect to cassandrahost at " + cassandraHost + "/" + cassandraPort);
        }

    }

    public void cleanup() {

        if (tr != null) {
            tr.close();
        }

        mutation_map = null;
        currentSize = 0;
    }


    /**
      * generate an random index from 0 to size-1 such that each hostname will map to the same
      * index consistently.
      *
      * @param hostname
      * @param size max size of index
      * @return an integer from 0 to size-1 (inclusive)
      */
     private int hashHost(String hostname, int size) {
         int s = 0;
         byte[] b = hostname.getBytes();

         for (byte a : b ) {
             s += (int) a;
         }
         return s % size;
     }
    

    /**
      * clear current batched operations.
      *
      */
    public void clear() {
        mutation_map = new HashMap<String, HashMap<String, LinkedList<Mutation>>>();
        currentSize = 0;
     }

     /**
      * insert new data operation.  inserts key[column] = value into mutation_map
      *
      * @param tableName the table in which to insert the column/value
      * @param key identifies the row in which to add the column/value
      * @param column the new column name to add
      * @param value the value to add for the column
      * @throws java.io.IOException if there is any error
      */
     public void insert(String tableName, String key, String column, int value) throws IOException {

         if (mutation_map == null) {
             clear();
         }

         long timestamp = System.currentTimeMillis();

         if (mutation_map.get(key) == null) {
             mutation_map.put(key, new HashMap<String, LinkedList<Mutation>>());
             (mutation_map.get(key)).put(tableName, new LinkedList<Mutation>());
         }

         Mutation kmerinsert = new Mutation();

         byte[] b = intToByteArray(value);

         ColumnOrSuperColumn c = new ColumnOrSuperColumn();
         c.setColumn(new Column(column.getBytes(), b, timestamp));
         kmerinsert.setColumn_or_supercolumn(c);

         ((mutation_map.get(key)).get(tableName)).add(kmerinsert);

     }

    /**
     * insert new data operation.  inserts key[column.subcolumn] = value
     *
     * @param key is the key for the table to insert (the tablename is specifed as config parameter)
     * @param column is the supercolumn name
     * @param subcolumn subcolumn name
     * @param value the string value to insert
     * @throws IOException if some error occurs.
     */
    public void insert(String key, String column, String subcolumn, String value) throws IOException {

        if (mutation_map == null) {
            clear();
        }

        long timestamp = System.currentTimeMillis();

        if (mutation_map.get(key) == null) {
            mutation_map.put(key, new HashMap());
            ((HashMap) mutation_map.get(key)).put(cassandraTable, new LinkedList());
        }

        Mutation kmerinsert = new Mutation();

        List<Column> lc = new LinkedList<Column>();
        lc.add(new Column(subcolumn.getBytes(), value.getBytes(), timestamp));

        ColumnOrSuperColumn c = new ColumnOrSuperColumn();
        c.setSuper_column(new SuperColumn(column.getBytes(), lc));
        kmerinsert.setColumn_or_supercolumn(c);

        ((List) ((HashMap) mutation_map.get(key)).get(cassandraTable)).add(kmerinsert);

        ++currentSize;
    }


     /**
      * flush data store operations to cassandra server, return (roughly) the number of bytes
      * sent.
      *
      * @return the number of bytes sent (roughly)
      * @throws IOException if there is some error
      */
     public int commit() throws IOException {

         try {
             client.batch_mutate(keyspace, (Map) mutation_map, ConsistencyLevel.ONE);
         } catch (Exception e) {
             throw new IOException(e);
         }
         return mutation_map.toString().length();

     }


    /**
     * convet int to byte array assuming 8 bytes per integer
     * @param value to convert
     * @return a fresh byte array
     */
    public static byte[] intToByteArray(int value) {
            byte[] b = new byte[8];
            for (int i = 0; i < 8; i++) {
                int offset = (b.length - 1 - i) * 8;
                b[i] = (byte) ((value >>> offset) & 0xFF);
            }
            return b;
        }

}

