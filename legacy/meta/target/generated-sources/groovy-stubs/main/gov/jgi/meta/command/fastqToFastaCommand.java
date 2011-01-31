//
// Generated stub from file:/Users/dev/dev/git.metagenomics/meta/src/main/groovy/gov/jgi/meta/command/fastqToFastaCommand.groovy
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
import gov.jgi.meta.MetaUtils;
import org.apache.hadoop.mapreduce.Job;
import gov.jgi.meta.ContigKmer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import gov.jgi.meta.hadoop.input.FastqInputFormat;
import gov.jgi.meta.hadoop.map.FastaIdentityMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * wrapper for the hadoop application
 */
public class fastqToFastaCommand
    extends java.lang.Object
    implements groovy.lang.GroovyObject, command
{
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
