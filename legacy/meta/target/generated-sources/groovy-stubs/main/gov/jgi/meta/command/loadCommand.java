//
// Generated stub from file:/Users/dev/dev/git.metagenomics/meta/src/main/groovy/gov/jgi/meta/command/loadCommand.groovy
//

package gov.jgi.meta.command;

import java.lang.*;
import java.io.*;
import java.net.*;
import java.util.*;
import groovy.lang.*;
import groovy.util.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.biojavax.bio.seq.RichSequenceIterator;
import org.biojavax.bio.seq.RichSequence;
import org.biojava.bio.BioException;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;

/**
 * loads sequence data into datastore. command looks like % meta load <table>
 *
 * @param -f file to read and load (or stdin)
 * @param -k keyspace to load into
 * @param -c cassandra host to connect to
 * @param -p port for host
 * @note the cassandra tables must be setup and existent. they must also have
the correct structure. (currently a supercolumn named "sequence").
 */
public class loadCommand
    extends java.lang.Object
    implements groovy.lang.GroovyObject, command
{
    private java.lang.String DEFAULTKEYSPACE = null;
    public java.lang.String getDEFAULTKEYSPACE() {
        throw new InternalError("Stubbed method");
    }
    public void setDEFAULTKEYSPACE(java.lang.String value) {
        throw new InternalError("Stubbed method");
    }

    private java.lang.String DEFAULTTABLE = null;
    public java.lang.String getDEFAULTTABLE() {
        throw new InternalError("Stubbed method");
    }
    public void setDEFAULTTABLE(java.lang.String value) {
        throw new InternalError("Stubbed method");
    }

    private java.lang.String DEFAULTHOST = null;
    public java.lang.String getDEFAULTHOST() {
        throw new InternalError("Stubbed method");
    }
    public void setDEFAULTHOST(java.lang.String value) {
        throw new InternalError("Stubbed method");
    }

    private int DEFAULTPORT = 0;
    public int getDEFAULTPORT() {
        throw new InternalError("Stubbed method");
    }
    public void setDEFAULTPORT(int value) {
        throw new InternalError("Stubbed method");
    }

    private List flags = null;
    public List getFlags() {
        throw new InternalError("Stubbed method");
    }
    public void setFlags(List value) {
        throw new InternalError("Stubbed method");
    }

    private List params = null;
    public List getParams() {
        throw new InternalError("Stubbed method");
    }
    public void setParams(List value) {
        throw new InternalError("Stubbed method");
    }

    public java.lang.String name() {
        throw new InternalError("Stubbed method");
    }

    public List options() {
        throw new InternalError("Stubbed method");
    }

    public java.lang.String usage() {
        throw new InternalError("Stubbed method");
    }

    /**
     * loads a set of sequence data into cassandra database
     *
     * @param arguments for the load command. arg[0] is load, arg[1] is the table
     * @param options specifier
     * @return 0 if all went well, not 0 if there was an error
     */
    public int execute(List args, Map options) {
        throw new InternalError("Stubbed method");
    }

    public groovy.lang.MetaClass getMetaClass() {
        throw new InternalError("Stubbed method");
    }

    public void setMetaClass(groovy.lang.MetaClass metaClass) {
        throw new InternalError("Stubbed method");
    }

    public java.lang.Object invokeMethod(java.lang.String name, java.lang.Object args) {
        throw new InternalError("Stubbed method");
    }

    public java.lang.Object getProperty(java.lang.String name) {
        throw new InternalError("Stubbed method");
    }

    public void setProperty(java.lang.String name, java.lang.Object value) {
        throw new InternalError("Stubbed method");
    }
}
